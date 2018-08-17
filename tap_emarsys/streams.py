import time
from functools import partial

import pendulum
import singer
from singer import metadata
from singer.bookmarks import write_bookmark, reset_stream
from ratelimit import limits, sleep_and_retry, RateLimitException
from backoff import on_exception, expo, constant

from .schemas import (
    IDS,
    get_contacts_raw_fields,
    normalize_fieldname,
    METRICS_AVAILABLE
)
from .http import MetricsRateLimitException

LOGGER = singer.get_logger()

MAX_METRIC_JOB_TIME = 1800
METRIC_JOB_POLL_SLEEP = 5

def count(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))

def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    count(tap_stream_id, records)

def get_date_and_integer_fields(stream):
    date_fields = []
    integer_fields = []
    for prop, json_schema in stream.schema.properties.items():
        _type = json_schema.type
        if isinstance(_type, list) and 'integer' in _type or \
           _type == 'integer':
           integer_fields.append(prop)
        elif json_schema.format == 'date-time':
            date_fields.append(prop)
    return date_fields, integer_fields

def base_transform(date_fields, integer_fields, obj):
    new_obj = {}
    for field, value in obj.items():
        if value == '':
            value = None
        elif field in integer_fields and value is not None:
            value = int(value)
        elif field in date_fields and value is not None:
            value = pendulum.parse(value).isoformat()
        new_obj[field] = value
    return new_obj

def select_fields(mdata, obj):
    new_obj = {}
    for key, value in obj.items():
        field_metadata = mdata.get(('properties', key))
        if field_metadata and \
           (field_metadata.get('selected') is True or \
            field_metadata.get('inclusion') == 'automatic'):
            new_obj[key] = value
    return new_obj

def sync_campaigns(ctx, sync):
    data = ctx.client.get('/email/', endpoint='campaigns', params={
        'showdeleted': 1
    })

    stream = ctx.catalog.get_stream('campaigns')
    date_fields, integer_fields = get_date_and_integer_fields(stream)

    data_transformed = list(map(partial(base_transform, date_fields, integer_fields), data))

    if sync:
        mdata = metadata.to_map(stream.metadata)
        data_selected = list(map(partial(select_fields, mdata), data_transformed))
        write_records('campaigns', data_selected)
    return data_transformed

def transform_contact(field_id_map, contact):
    new_obj = {}
    for field_id, value in contact.items():
        if field_id in ['id', 'uid']:
            new_obj[field_id] = value
            continue
        field_info = field_id_map[field_id]
        if value == '' or value is None:
            value = None
        elif field_info['type'] == 'date':
            value = pendulum.parse(value).isoformat()
        elif field_info['type'] == 'numeric':
            value = float(value)
        new_obj[field_info['name']] = value
    return new_obj

def sync_contacts_page(ctx, field_id_map, selected_fields, limit, offset):
    LOGGER.info('contacts - Syncing page - limit: {}, offset: {}'.format(limit, offset))

    contact_list_page = ctx.client.get(
        '/contact/query/',
        endpoint='contacts_list',
        params={
            'return': 3,
            'limit': limit,
            'offset': offset
        })

    if contact_list_page['errors']:
        raise Exception('contacts - {}'.format(','.join(contact_list_page['errors'])))

    query = {
        'keyId': 'id',
        'keyValues': list(map(lambda x: x['id'], contact_list_page['result'])),
        'fields': selected_fields
    }
    contact_page = ctx.client.post('/contact/getdata', query, endpoint='contacts')

    contacts = list(map(partial(transform_contact, field_id_map), contact_page['result']))
    write_records('contacts', contacts)

    return len(contact_page['result'])

def sync_contacts(ctx):
    contacts_stream = ctx.catalog.get_stream('contacts')
    max_pages = ctx.config.get('max_pages')
    if max_pages:
        max_pages = int(max_pages)

    raw_fields = get_contacts_raw_fields(ctx)
    # sort so fields are processed in created order, more recent duplicates are appended with _{id}
    raw_fields = sorted(raw_fields, key=lambda x: x['id'])
    field_name_map = {}
    field_id_map = {}
    for raw_field in raw_fields:
        field_id = str(raw_field['id'])
        field_name = normalize_fieldname(raw_field['name'])
        if field_name in field_name_map:
            field_name += '_' + str(field_id)

        field_info = {
            'type': raw_field['application_type'],
            'name': field_name,
            'id': field_id
        }
        field_name_map[field_name] = field_info
        field_id_map[field_id] = field_info
    raw_fields_available = list(field_name_map.keys())

    selected_field_maps = []
    for metadata_entry in contacts_stream.metadata:
        breadcrumb = metadata_entry.get('breadcrumb')
        if breadcrumb and breadcrumb[0] == 'properties':
            field_name = breadcrumb[1]
            field_metadata = metadata_entry.get('metadata')
            if field_name not in ['id', 'uid'] and \
              (field_metadata.get('selected') is True or \
               field_metadata.get('inclusion') == 'automatic'):
                if field_name not in raw_fields_available:
                    raise Exception('Field `{}` not currently available from Emarsys'.format(
                        field_name))
                selected_field_maps.append(field_name_map[field_name])

    if not selected_field_maps:
        selected_fields = ['3'] # no selected fields fetches all
    else:
        selected_fields = list(map(lambda x: x['id'], selected_field_maps))

    limit = 1000
    count = limit
    offset = 0
    while count == limit and \
          (max_pages is None or (offset / limit) <= max_pages):
        count = sync_contacts_page(ctx, field_id_map, selected_fields, limit, offset)
        offset += limit

def sync_contact_lists(ctx, sync):
    data = ctx.client.get('/contactlist', endpoint='contact_lists')

    stream = ctx.catalog.get_stream('contact_lists')
    date_fields, integer_fields = get_date_and_integer_fields(stream)
    data_transformed = list(map(partial(base_transform, date_fields, integer_fields), data))

    if sync:
        mdata = metadata.to_map(stream.metadata)
        data_selected = list(map(partial(select_fields, mdata), data_transformed))
        write_records('contact_lists', data_selected)
    return data_transformed

def sync_contact_list_memberships(ctx, contact_list_id, limit, offset):
    LOGGER.info('contact_list_memberships - Syncing page - list_id: {}, limit: {}, offset: {}'.format(
                    contact_list_id,
                    limit,
                    offset))

    membership_ids = ctx.client.get('/contactlist/{}/'.format(contact_list_id),
                                    params={
                                        'limit': limit,
                                        'offset': offset
                                    },
                                    endpoint='contact_list_memberships')
    memberships = []
    for membership_id in membership_ids:
        memberships.append({
            'contact_list_id': contact_list_id,
            'contact_id': membership_id
        })
    write_records('contact_list_memberships', memberships)

    return len(memberships)

def sync_contact_lists_memberships(ctx, contact_lists):
    max_pages = ctx.config.get('max_pages')
    if max_pages:
        max_pages = int(max_pages)

    if max_pages:
        contact_lists = contact_lists[:max_pages]

    for contact_list in contact_lists:
        limit = 1000000
        count = limit
        offset = 0
        while count == limit and \
              (max_pages is None or (offset / limit) <= max_pages):
            count = sync_contact_list_memberships(ctx, contact_list['id'], limit, offset)
            offset += limit

@on_exception(constant, MetricsRateLimitException, max_tries=5, interval=60)
@on_exception(expo, RateLimitException, max_tries=5)
@sleep_and_retry
@limits(calls=1, period=61) # 60 seconds needed to be padded by 1 second to work
def post_metric(ctx, metric, start_date, end_date, campaign_id):
    LOGGER.info('Metrics query - metric: {} start_date: {} end_date: {} campaign_id: {}'.format(
        metric,
        start_date,
        end_date,
        campaign_id))
    return ctx.client.post(
        '/email/responses',
        {
            'type': metric,
            'start_date': start_date,
            'end_date': end_date,
            'campaign_id': campaign_id
        },
        endpoint='metrics_job')

def sync_metric(ctx, campaign_id, metric, start_date, end_date):
    with singer.metrics.job_timer('daily_aggregated_metric'):
        job = post_metric(ctx,
                          metric,
                          start_date.to_date_string(),
                          end_date.to_date_string(),
                          campaign_id)

        LOGGER.info('Metrics query job - {}'.format(job['id']))

        start = time.monotonic()
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            LOGGER.info('Polling metrics query job - {}'.format(job['id']))
            data = ctx.client.get('/email/{}/responses'.format(job['id']), endpoint='metrics')
            if data != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    if len(data['contact_ids']) == 1 and data['contact_ids'][0] == '':
        return

    data_rows = []
    metric_date = start_date.isoformat()
    for contact_id in data['contact_ids']:
        data_rows.append({
            'date': metric_date,
            'metric': metric,
            'contact_id': contact_id,
            'campaign_id': campaign_id
        })

    write_records('metrics', data_rows)

def write_metrics_state(ctx, campaigns_to_resume, metrics_to_resume, date_to_resume):
    write_bookmark(ctx.state, 'metrics', 'campaigns_to_resume', campaigns_to_resume)
    write_bookmark(ctx.state, 'metrics', 'metrics_to_resume', metrics_to_resume)
    write_bookmark(ctx.state, 'metrics', 'date_to_resume', date_to_resume.to_date_string())
    ctx.write_state()

def sync_metrics(ctx, campaigns):
    max_pages = ctx.config.get('max_pages')
    if max_pages:
        max_pages = int(max_pages)
    stream = ctx.catalog.get_stream('metrics')
    bookmark = ctx.state.get('bookmarks', {}).get('metrics', {})

    if stream.metadata:
        mdata = metadata.to_map(stream.metadata)
        metrics_selected = (
            mdata
            .get((), {})
            .get('tap-emarsys.metrics-selected', METRICS_AVAILABLE)
        )
    else:
        metrics_selected = METRICS_AVAILABLE

    start_date = pendulum.parse(ctx.config.get('start_date', 'now'))
    end_date = pendulum.parse(ctx.config.get('end_date', 'now'))

    start_date = bookmark.get('last_metric_date', start_date)

    campaigns_to_resume = bookmark.get('campaigns_to_resume')
    if campaigns_to_resume:
        campaign_ids = campaigns_to_resume
        last_metrics = bookmark.get('metrics_to_resume')
        last_date = bookmark.get('date_to_resume')
        if last_date:
            last_date = pendulum.parse(last_date)
    else:
        campaign_ids = (
            list(map(lambda x: x['id'],
                     filter(lambda x: x['deleted'] is None,
                            campaigns)))
        )
        metrics_to_resume = metrics_selected
        last_date = None
        last_metrics = None

    current_date = last_date or start_date

    if max_pages:
        end_date = current_date
        metrics_selected = metrics_selected[:max_pages]

    while current_date <= end_date:
        next_date = current_date.add(days=1)
        campaigns_to_resume = campaign_ids.copy()
        for campaign_id in campaign_ids:
            campaign_metrics = last_metrics or metrics_selected
            last_metrics = None
            metrics_to_resume = campaign_metrics.copy()
            for metric in campaign_metrics:
                sync_metric(ctx,
                            campaign_id,
                            metric,
                            current_date,
                            next_date)
                write_metrics_state(ctx, campaigns_to_resume, metrics_to_resume, current_date)
                metrics_to_resume.remove(metric)
            campaigns_to_resume.remove(campaign_id)
        current_date = next_date

    reset_stream(ctx.state, 'metrics')
    write_bookmark(ctx.state, 'metrics', 'last_metric_date', end_date.to_date_string())
    ctx.write_state()

def sync_selected_streams(ctx):
    selected_streams = ctx.selected_stream_ids
    last_synced_stream = ctx.state.get('last_synced_stream')

    if IDS.CONTACTS in selected_streams and last_synced_stream != IDS.CONTACTS:
        sync_contacts(ctx)
        ctx.state['last_synced_stream'] = IDS.CONTACTS
        ctx.write_state()

    if (IDS.CONTACT_LISTS in selected_streams and \
        last_synced_stream != IDS.CONTACT_LISTS) or \
       (IDS.CONTACT_LIST_MEMBERSHIPS in selected_streams and \
        last_synced_stream != IDS.CONTACT_LIST_MEMBERSHIPS):
        contact_lists = sync_contact_lists(ctx, IDS.CONTACT_LISTS in selected_streams)
        ctx.state['last_synced_stream'] = IDS.CONTACT_LISTS
        ctx.write_state()

    if IDS.CONTACT_LIST_MEMBERSHIPS in selected_streams and \
       last_synced_stream != IDS.CONTACT_LIST_MEMBERSHIPS:
        sync_contact_lists_memberships(ctx, contact_lists)
        ctx.state['last_synced_stream'] = IDS.CONTACT_LIST_MEMBERSHIPS
        ctx.write_state()

    if (IDS.CAMPAIGNS in selected_streams and \
        last_synced_stream != IDS.CAMPAIGNS) or \
       (IDS.METRICS in selected_streams and \
        last_synced_stream != IDS.METRICS):
        campaigns = sync_campaigns(ctx, IDS.CAMPAIGNS in selected_streams)
        ctx.state['last_synced_stream'] = IDS.CAMPAIGNS
        ctx.write_state()

    if IDS.METRICS in selected_streams and last_synced_stream != IDS.METRICS:
        sync_metrics(ctx, campaigns)
        ctx.state['last_synced_stream'] = IDS.METRICS
        ctx.write_state()

    ctx.state['last_synced_stream'] = None
    ctx.write_state()
