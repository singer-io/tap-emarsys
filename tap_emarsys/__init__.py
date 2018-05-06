#!/usr/bin/env python3

import os
import json

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams
from .context import Context
from . import schemas

REQUIRED_CONFIG_KEYS = ["username", "secret"]

LOGGER = singer.get_logger()

def check_credentials_are_authorized(ctx):
    ctx.client.get('/settings')

def discover(ctx):
    check_credentials_are_authorized(ctx)
    catalog = Catalog([])
    for tap_stream_id in schemas.static_schema_stream_ids:
        schema = Schema.from_dict(schemas.load_schema(tap_stream_id),
                                  inclusion="automatic")
        metadata = []
        if tap_stream_id in schemas.ROOT_METADATA:
            metadata.append(schemas.ROOT_METADATA[tap_stream_id])
        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
            metadata=metadata
        ))
    contacts_schema = schemas.get_contacts_schema(ctx)
    catalog.streams.append(CatalogEntry(
        stream='contacts',
        tap_stream_id='contacts',
        key_properties=schemas.PK_FIELDS['contacts'],
        schema=contacts_schema
    ))

    catalog.dump()

def sync(ctx):
    for tap_stream_id in schemas.static_schema_stream_ids:
        schemas.load_and_write_schema(tap_stream_id)
    streams.sync_selected_streams(ctx)
    ctx.write_state()

@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)
    if args.discover:
        discover(ctx)
    else:
        ctx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover(ctx)
        sync(ctx)

if __name__ == "__main__":
    main()
