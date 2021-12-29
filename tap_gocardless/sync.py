#!/usr/bin/env python3
import copy
from typing import Callable, Iterable

import gocardless_pro
import singer
from singer import Catalog

from tap_gocardless import tools
from tap_gocardless.streams import STREAMS

LOGGER = singer.get_logger()


def sync(
    gocardless_client: gocardless_pro.Client,
    config: dict,
    state: dict,
    catalog: Catalog,
):
    LOGGER.debug("###### DEBUG #######")
    LOGGER.debug(f"{config=}")
    LOGGER.debug(f"{state=}")
    LOGGER.debug(f"{catalog.to_dict()=}")
    LOGGER.debug("###### DEBUG #######")

    """Sync data from tap source"""
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state) :

        if STREAMS[stream.tap_stream_id].get('is_child', False):
            LOGGER.info("Syncing child stream later:" + stream.tap_stream_id)
        else:

            LOGGER.debug("###### DEBUG #######")
            LOGGER.debug(f"{stream.to_dict()=}")
            LOGGER.debug("###### DEBUG #######")

            LOGGER.info("Syncing stream:" + stream.tap_stream_id)

            # Update the current stream as active syncing in the state
            singer.set_currently_syncing(state, stream.tap_stream_id)

            # Retrieve the state of the stream
            stream_state: dict = tools.get_stream_state(
                state,
                stream.tap_stream_id,
            )

            # If replication_key is empty set default value to start_date
            if stream.replication_key not in stream_state:
                stream_state[stream.replication_key] = config.get("start_date")

            LOGGER.info(f"Stream state: {stream_state}")

            # Write the schema
            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=stream.schema.to_dict(),
                key_properties=stream.key_properties,
            )

            # Before factorization : this how we get records for one stream
            # tap_data = gocardless_client.events.list().records

            # Every stream has a corresponding method in the GoCardLess object e.g.:
            # stream.tap_stream_id = events
            # tap_data: Callable = getattr(gocardless_client, stream.tap_stream_id)
            # tap_data ==> gocardless_client.events

            tap_data: Callable = getattr(gocardless_client, stream.tap_stream_id)
            # tap_data: gocardless_client.events = getattr(gocardless_client, stream.tap_stream_id)

            rows = []
            current_initial_full_table_complete = singer.get_bookmark(
                state, stream.tap_stream_id, "initial_full_table_complete", False
            )
            LOGGER.info(
                f"replication_method is {stream.replication_method} and initial_full_table_complete is {current_initial_full_table_complete} for :"
                + stream.tap_stream_id
            )

            if (
                stream.replication_method == "INCREMENTAL"
                or current_initial_full_table_complete
            ):
                stream_state.pop("initial_full_table_complete", None)
                rows = get_incremental_rows(tap_data, stream_state, stream)

            elif stream.replication_method == "FULL_TABLE":
                rows = get_full_rows(tap_data, stream)
            else:
                raise Exception(
                    "only FULL_TABLE, and INCREMENTAL replication \
                    methods are supported (you passed {})".format(
                        stream.replication_method
                    )
                )

            ids_for_child = None
            stream_child = None

            if "child" in STREAMS[stream.tap_stream_id]:
                ids_for_child = set()
                stream_child = STREAMS[stream.tap_stream_id]["child"]

            max_bookmark = None
            for row in rows:
                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, [row])

                if "bookmark" in STREAMS[stream.tap_stream_id]:
                    if not max_bookmark:
                        max_bookmark = row[STREAMS[stream.tap_stream_id]["bookmark"]]
                    max_bookmark = max(
                        max_bookmark, row[STREAMS[stream.tap_stream_id]["bookmark"]]
                    )

                    singer.write_bookmark(
                        state, stream.tap_stream_id, stream.replication_key, max_bookmark
                    )

                    # keep key_properties (id) for child
                    if stream_child is not None:
                        ids_for_child.add(row[STREAMS[stream.tap_stream_id]['key_properties']])

            state = singer.write_bookmark(
                state, stream.tap_stream_id, "initial_full_table_complete", True
            )

            # Clear currently syncing
            tools.clear_currently_syncing(state)

            # Write the bootmark
            singer.write_state(state)

            # Sync child
            if stream_child is not None:
                sync_child(gocardless_client, config, state, catalog,    stream_child,  ids_for_child)

            LOGGER.debug("###### DEBUG #######")
            LOGGER.debug(f"{state=}")
            LOGGER.debug("###### DEBUG #######")

    return


def get_incremental_rows(tap_data: Callable, stream_state: dict, stream):
    params = copy.deepcopy(stream_state)
    params['limit'] = 100
    more_records = True
    total_cnt = 0
    while more_records:
        cnt = 0
        for row in tap_data.list(params=params).records:
            cnt = cnt + 1
            yield row.attributes

        LOGGER.info(f"Last API call found {cnt} records for {stream.tap_stream_id}")
        total_cnt = total_cnt + cnt
        if cnt == 0:
            LOGGER.info(f"No more records for {stream.tap_stream_id}")
            more_records = False
        else:
            LOGGER.info(f"Last id is {row.attributes['id']} for {stream.tap_stream_id}")
            LOGGER.debug(f"Last record is {row.attributes} for {stream.tap_stream_id}")
            params["after"] = row.attributes["id"]

    LOGGER.info(f"Total received records {total_cnt} for {stream.tap_stream_id}")


def get_full_rows(tap_data: Callable, stream) -> Iterable:
    total_cnt = 0
    for row in tap_data.all():
        total_cnt = total_cnt + 1
        yield row.attributes

    LOGGER.info(f"Total received records {total_cnt} for {stream.tap_stream_id}")

def get_individual_rows(tap_data: Callable, stream, key, ids) -> Iterable:

    total_cnt = 0
    for id in  ids:
        for row in tap_data.list(params={key:id}).records:
            yield row.attributes
            total_cnt = total_cnt + 1

    LOGGER.info(f"Total received records {total_cnt} for {stream.tap_stream_id}")



def sync_child( gocardless_client: gocardless_pro.Client,
    config: dict,
    state: dict,
    catalog: Catalog,
    stream_child,
    ids):

    for stream in catalog.get_selected_streams(state):
        if stream.tap_stream_id == stream_child:

            LOGGER.info("Syncing child stream:" + stream.tap_stream_id)

            # Write the schema
            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=stream.schema.to_dict(),
                key_properties=stream.key_properties,
            )

            tap_data: Callable = getattr(gocardless_client, stream.tap_stream_id)

            for row in get_individual_rows(tap_data, stream, STREAMS[stream.tap_stream_id]['key_properties'], ids):
                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, [row])
