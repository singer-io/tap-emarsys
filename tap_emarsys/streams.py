from functools import partial

import singer
import pendulum

from .schemas import IDS, get_contacts_raw_fields, get_contact_field_type, normalize_fieldname

LOGGER = singer.get_logger()

def metrics(tap_stream_id, records):  
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))

def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    metrics(tap_stream_id, records)

def base_transform(obj, date_fields):
    new_obj = {}
    for field, value in obj.items():
        if value == '':
            value = None
        elif field in date_fields and value is not None:
            value = pendulum.parse(value).isoformat()
        new_obj[field] = value
    return new_obj

def sync_campaigns(ctx):
    data = ctx.client.get('/email/', tap_stream_id='campaigns', params={
        'showdeleted': 1
    })
    def campaign_transformed(campaign):
        return base_transform(campaign, ['created', 'deleted'])
    data_transformed = list(map(campaign_transformed, data))
    ## TODO: select fields?
    write_records('campaigns', data_transformed)

def transform_contact(field_id_map, contact):
    new_obj = {}
    for field_id, value in contact.items():
        if field_id in ['id', 'uid']:
            new_obj[field_id] = value
            continue
        field_info = field_id_map[field_id]
        if value == '':
            value = None
        elif field_info['type'] == 'date':
            value = pendulum.parse(value).isoformat()
        new_obj[field_info['name']] = value
    return new_obj

def paginate_contacts(ctx, field_id_map, selected_fields, limit=1000, offset=0):
    contact_list_page = ctx.client.get('/contact/query/', params={
        'return': 3,
        'limit': limit,
        'offset': offset
    })
    if len(contact_list_page['errors']) > 0:
        raise Exception('contacts - {}'.format(','.join(contact_list_page['errors'])))

    query = {
        'keyId': 'id',
        'keyValues': list(map(lambda x: x['id'], contact_list_page['result'])),
        'fields': list(map(lambda x: x['id'], selected_fields))
    }
    contact_page = ctx.client.post('/contact/getdata', query, tap_stream_id='contacts')

    contacts = list(map(partial(transform_contact, field_id_map), contact_page['result']))
    write_records('contacts', contacts)

    if len(contact_page['result']) == limit:
        paginate_contacts(ctx, field_id_map, selected_fields, limit=limit, offset=offset + limit)

def sync_contacts(ctx):
    contacts_stream = ctx.catalog.get_stream('contacts')

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

    selected_fields = []
    for prop, schema in contacts_stream.schema.properties.items():
        if schema.selected == True:
            if prop not in raw_fields_available:
                raise Exception('Field `{}` not currently available from Emarsys'.format(
                    prop))
            selected_fields.append(field_name_map[prop])

    paginate_contacts(ctx, field_id_map, selected_fields)

def sync_contact_lists(ctx, sync):
    data = ctx.client.get('/contactlist', tap_stream_id='contact_lists')
    ## TODO: select fields?
    def contact_list_transform(contact_list):
        return base_transform(contact_list, ['created'])
    data_transformed = list(map(contact_list_transform, data))
    if sync:
        write_records('contact_lists', data_transformed)
    return data_transformed

def sync_contact_list_memberships(ctx, contact_list_id, limit=1000000, offset=0):
    membership_ids = ctx.client.get('/contactlist/{}/'.format(contact_list_id),
                                    params={
                                        'limit': limit,
                                        'offset': offset
                                    },
                                    tap_stream_id='contact_list_memberships')
    memberships = []
    for membership_id in membership_ids:
        memberships.append({
            'contact_list_id': contact_list_id,
            'contact_id': membership_id
        })
    write_records('contact_list_memberships', memberships)

    if len(membership_ids) == limit:
        sync_contact_list_memberships(ctx, contact_list_id, limit=limit, offset=offset + limit)

def sync_contact_lists_memberships(ctx, contact_lists):
    for contact_list in contact_lists:
        sync_contact_list_memberships(ctx, contact_list['id'])

def sync_selected_streams(ctx):
    selected_streams = ctx.selected_stream_ids

    if IDS.CAMPAIGNS in selected_streams:
        sync_campaigns(ctx)
    if IDS.CONTACTS in selected_streams:
        sync_contacts(ctx)
    if IDS.CONTACT_LISTS in selected_streams or \
       IDS.CONTACT_LIST_MEMBERSHIPS in selected_streams:
        contact_lists = sync_contact_lists(ctx, IDS.CONTACT_LISTS in selected_streams)
    if IDS.CONTACT_LIST_MEMBERSHIPS in selected_streams:
        sync_contact_lists_memberships(ctx, contact_lists)
