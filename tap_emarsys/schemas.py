import os
import re

import singer
from singer import utils
from singer.catalog import Schema

class IDS(object):
    CAMPAIGNS = 'campaigns'
    CONTACTS = 'contacts'

static_schema_stream_ids = [IDS.CAMPAIGNS]

PK_FIELDS = {
    IDS.CAMPAIGNS: ['id'],
    IDS.CONTACTS: ['id']
}

def normalize_fieldname(fieldname):
    fieldname = fieldname.lower()
    fieldname = re.sub(r'[\s\-]', '_', fieldname)
    return re.sub(r'[^a-z0-9_]', '', fieldname)

def get_contact_field_type(raw_field_type):
    if raw_field_type == 'date':
        return 'string', 'date-time'
    if raw_field_type == 'numeric':
        return 'number', None
    return 'string', None

def get_contacts_raw_fields(ctx):
    return ctx.client.get('/field')

def get_contacts_schema(ctx):
    raw_fields = get_contacts_raw_fields(ctx)
    properties = {}
    for raw_field in raw_fields:
        _type, _format = get_contact_field_type(raw_field['application_type'])
        json_schema = {
            'type': ['null', _type],
            'inclusion': 'available'
        }
        if _format is not None:
            json_schema['format'] = _format
        properties[normalize_fieldname(raw_field['name'])] = json_schema

    properties['id'] = {
        'type': ['string'],
        'inclusion': 'automatic'
    }
    properties['uid'] = {
        'type': ['string'],
        'inclusion': 'automatic'
    }

    schema = {
        'type': ['object'],
        'additionalProperties': False,
        'properties': properties
    }

    return Schema.from_dict(schema)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(tap_stream_id):
    path = 'schemas/{}.json'.format(tap_stream_id)
    return utils.load_json(get_abs_path(path))

def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])
