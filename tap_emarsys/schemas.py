import os
import re

import singer
from singer import utils
from singer.catalog import Schema

class IDS(object): # pylint: disable=too-few-public-methods
    CAMPAIGNS = 'campaigns'
    CONTACTS = 'contacts'
    CONTACT_LISTS = 'contact_lists'
    CONTACT_LIST_MEMBERSHIPS = 'contact_list_memberships'
    METRICS = 'metrics'

STATIC_SCHEMA_STREAM_IDS = [
    IDS.CAMPAIGNS,
    IDS.CONTACT_LISTS,
    IDS.CONTACT_LIST_MEMBERSHIPS,
    IDS.METRICS
]

PK_FIELDS = {
    IDS.CAMPAIGNS: ['id'],
    IDS.CONTACTS: ['id'],
    IDS.CONTACT_LISTS: ['id'],
    IDS.CONTACT_LIST_MEMBERSHIPS: ['contact_list_id', 'contact_id'],
    IDS.METRICS: ['date', 'metric', 'contact_id', 'campaign_id']
}

METRICS_AVAILABLE = [
    'opened',
    'not_opened',
    'received',
    'clicked',
    'not_clicked',
    'bounced',
    'hard_bounced',
    'soft_bounced',
    'block_bounced'
]

ROOT_METADATA = {
    IDS.METRICS: {
        'metadata': {
            'tap-emarsys.metrics-available': METRICS_AVAILABLE
        },
        'breadcrumb': []
    }
}

def normalize_fieldname(fieldname):
    fieldname = fieldname.lower()
    fieldname = re.sub(r'[\s\-]', '_', fieldname)
    return re.sub(r'[^a-z0-9_]', '', fieldname)

def get_contact_json_schema(raw_field_type):
    if raw_field_type == 'date':
        return {
            'type': ['null', 'string'],
            'format': 'date-time'
        }
    if raw_field_type == 'numeric':
        return {
            'type': ['null', 'number']
        }
    if raw_field_type == 'special':
        return {
            'type': ['null', 'array', 'string'],
            'items': {
                'type': 'string'
            }
        }
    if raw_field_type == 'multichoice':
        return {
            'type': ['null', 'array'],
            'items': {
                'type': 'integer'
            }
        }
    return {
        'type': ['null', 'string']
    }

def get_contacts_raw_fields(ctx):
    return ctx.client.get('/field', endpoint='contact_fields')

def get_contacts_schema(ctx):
    raw_fields = get_contacts_raw_fields(ctx)
    # sort so fields are processed in created order, more recent duplicates are appended with _{id}
    raw_fields = sorted(raw_fields, key=lambda x: x['id'])
    properties = {}
    metadata = []
    for raw_field in raw_fields:
        field_id = raw_field['id']
        json_schema = get_contact_json_schema(raw_field['application_type'])

        field_name = normalize_fieldname(raw_field['name'])
        if field_name in properties:
            field_name += '_' + str(field_id)
        
        properties[field_name] = json_schema
        metadata.append({
            'metadata': {
                'inclusion': 'available',
                'tap-emarsys.field-id': field_id
            },
            'breadcrumb': ['properties', field_name]
        })

    for field_name in ['id', 'uid']:
        properties[field_name] = {'type': ['string']}
        metadata.append({
            'metadata': {
                'inclusion': 'automatic'
            },
            'breadcrumb': ['properties', field_name]
        })

    schema = {
        'type': ['object'],
        'additionalProperties': False,
        'properties': properties
    }

    return Schema.from_dict(schema), metadata

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(tap_stream_id):
    path = 'schemas/{}.json'.format(tap_stream_id)
    return utils.load_json(get_abs_path(path))

def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])
