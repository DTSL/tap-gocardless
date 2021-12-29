#!/usr/bin/env python3
import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry

from tap_gocardless.schema import load_schemas
from tap_gocardless.streams import STREAMS

LOGGER = singer.get_logger()


def discover():
    raw_schemas: dict = load_schemas()
    streams: list = []

    for stream_id, schema in raw_schemas.items():
        stream_metadata: dict = STREAMS[stream_id]

        # Create metadata
        mdata: list = metadata.get_standard_metadata(
            schema=schema.to_dict(),
            key_properties=stream_metadata.get("key_properties", None),
            valid_replication_keys=stream_metadata.get(
                "replication_keys",
                None,
            ),
            replication_method=stream_metadata.get(
                "replication_method",
                None,
            ),
        )

        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=mdata,
                replication_key=stream_metadata.get(
                    "replication_key",
                    None,
                ),
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=stream_metadata.get(
                    "replication_method",
                    None,
                ),
            )
        )
    return Catalog(streams)
