#!/usr/bin/env python3

import os
import sys
import json

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams
from .context import Context
from . import schemas

REQUIRED_CONFIG_KEYS = ["username", "secret"]

LOGGER = singer.get_logger()

def check_authorization(ctx):
    ctx.client.get('/settings')

def discover(ctx):
    check_authorization(ctx)
    catalog = Catalog([])
    for tap_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
        schema = Schema.from_dict(schemas.load_schema(tap_stream_id))
        metadata = []
        if tap_stream_id in schemas.ROOT_METADATA:
            metadata.append(schemas.ROOT_METADATA[tap_stream_id])
        for field_name in schema.properties.keys():
            if field_name in schemas.PK_FIELDS[tap_stream_id]:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            metadata.append({
                'metadata': {
                    'inclusion': inclusion
                },
                'breadcrumb': ['properties', field_name]
            })
        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
            metadata=metadata
        ))
    contacts_schema, contact_metadata = schemas.get_contacts_schema(ctx)
    catalog.streams.append(CatalogEntry(
        stream='contacts',
        tap_stream_id='contacts',
        key_properties=schemas.PK_FIELDS['contacts'],
        schema=contacts_schema,
        metadata=contact_metadata
    ))

    return catalog

def sync(ctx):
    for tap_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
        schemas.load_and_write_schema(tap_stream_id)
    contacts_schema, _ = schemas.get_contacts_schema(ctx)
    singer.write_schema('contacts',
                        contacts_schema.to_dict(),
                        schemas.PK_FIELDS['contacts'])

    streams.sync_selected_streams(ctx)
    ctx.write_state()

@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)
    if args.discover:
        catalog = discover(ctx)
        json.dump(catalog.to_dict(), sys.stdout)
    else:
        ctx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover(ctx)
        sync(ctx)

if __name__ == "__main__":
    main()
