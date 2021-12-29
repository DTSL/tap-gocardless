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
    #####################
    LOGGER.info("###### DEBUG #######")
    LOGGER.info(f"{config=}")
    LOGGER.info(f"{state=}")
    LOGGER.info(f"{catalog.to_dict()=}")
    LOGGER.info("###### DEBUG #######")
    ###################

    """Sync data from tap source"""
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("###### DEBUG #######")
        LOGGER.info(f"{stream.to_dict()=}")
        LOGGER.info("###### DEBUG #######")
        ###################

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
            stream_state.pop("initial_full_table_complete")
            # rows = get_incremental_rows(tap_data, stream_state, stream)
            pass
        elif stream.replication_method == "FULL_TABLE":
            # rows = get_full_rows(tap_data, stream)
            pass
        else:
            raise Exception(
                "only FULL_TABLE, and INCREMENTAL replication \
                methods are supported (you passed {})".format(
                    stream.replication_method
                )
            )

        for row in rows:
            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])

            if "bookmark" in STREAMS[stream.tap_stream_id]["bookmark"]:
                max_bookmark = max(
                    max_bookmark, row[STREAMS[stream.tap_stream_id]["bookmark"]]
                )

                state = singer.write_bookmark(
                    state, stream.tap_stream_id, stream.replication_key, max_bookmark
                )

        state = singer.write_bookmark(
            state, stream.tap_stream_id, "initial_full_table_complete", True
        )

        # Clear currently syncing
        tools.clear_currently_syncing(state)

        # Write the bootmark
        singer.write_state(state)

        LOGGER.info(f"BBA: {state=}")

    return


def get_incremental_rows(tap_data: Callable, stream_state: dict, stream):
    params = copy.deepcopy(stream_state)
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
