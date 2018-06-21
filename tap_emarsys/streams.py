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

def base_transform(date_fields, obj):
    new_obj = {}
    for field, value in obj.items():
        if value == '':
            value = None
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
    data_transformed = list(map(partial(base_transform, ['created', 'deleted']), data))
    if sync:
        stream = ctx.catalog.get_stream('campaigns')
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
        new_obj[field_info['name']] = value
    return new_obj

def paginate_contacts(ctx, field_id_map, selected_fields, limit=1000, offset=0, max_page=None):
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

    if len(contact_page['result']) == limit and \
       (max_page is None or (offset / limit) <= max_page):
        paginate_contacts(ctx,
                          field_id_map,
                          selected_fields,
                          limit=limit,
                          offset=offset + limit,
                          max_page=max_page)

def sync_contacts(ctx):
    contacts_stream = ctx.catalog.get_stream('contacts')
    test_mode = ctx.config.get('test_mode')

    raw_fields = get_contacts_raw_fields(ctx)
    field_name_map = {}
    field_id_map = {}
    for raw_field in raw_fields:
        field_id = str(raw_field['id'])
        field_name = normalize_fieldname(raw_field['name'])
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

    if test_mode:
        max_page = 3
    else:
        max_page = None

    paginate_contacts(ctx, field_id_map, selected_fields, max_page=max_page)

def sync_contact_lists(ctx, sync):
    data = ctx.client.get('/contactlist', endpoint='contact_lists')
    data_transformed = list(map(partial(base_transform, ['created']), data))
    if sync:
        stream = ctx.catalog.get_stream('contact_lists')
        mdata = metadata.to_map(stream.metadata)
        data_selected = list(map(partial(select_fields, mdata), data_transformed))
        write_records('contact_lists', data_selected)
    return data_transformed

def sync_contact_list_memberships(ctx, contact_list_id, limit=1000000, offset=0, max_page=None):
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

    if len(membership_ids) == limit:
        sync_contact_list_memberships(ctx,
                                      contact_list_id,
                                      limit=limit,
                                      offset=offset + limit,
                                      max_page=max_page)

def sync_contact_lists_memberships(ctx, contact_lists):
    test_mode = ctx.config.get('test_mode')
    if test_mode:
        contact_lists = contact_lists[:2]
        max_page = 2
    else:
        max_page = None

    for contact_list in contact_lists:
        sync_contact_list_memberships(ctx, contact_list['id'], max_page=max_page)

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
    test_mode = ctx.config.get('test_mode')
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

    if test_mode:
        end_date = current_date
        metrics_selected = metrics_selected[:2]

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
