import singer
import pendulum
from .schemas import IDS

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
    data = ctx.client.get('campaigns', '/email/', params={
        'showdeleted': 1
    })
    def campaign_transformed(campaign):
        return base_transform(campaign, ['created', 'deleted'])
    data_transformed = list(map(campaign_transformed, data))
    write_records('campaigns', data_transformed)

STREAM_SYNCS = {
    'campaigns': sync_campaigns
}

def sync_selected_streams(ctx):
    selected_streams = ctx.selected_stream_ids

    for stream in STREAM_SYNCS.keys():
        if stream in selected_streams:
            STREAM_SYNCS[stream](ctx)
