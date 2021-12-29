#!/usr/bin/env python3

import gocardless_pro
import singer
from singer import utils

from tap_gocardless.discover import discover
from tap_gocardless.sync import sync

REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Initialize Gocardless client
    gocardless_client: gocardless_pro.Client = gocardless_pro.Client(
        access_token=args.config["access_token"],
        environment=args.config.get("environment", "live"),
    )

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
        return
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

    sync(gocardless_client, args.config, args.state, catalog)


if __name__ == "__main__":
    main()
