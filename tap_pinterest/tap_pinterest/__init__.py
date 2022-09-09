#!/usr/bin/env python3
import singer
from tap_pinterest.client import PinterestClient
from tap_pinterest.discover import do_discover
from tap_pinterest.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'client_id',
    'client_secret',
    'refresh_token'
]


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with PinterestClient(
        client_id=parsed_args.config['client_id'],
        client_secret=parsed_args.config['client_secret'],
        refresh_token=parsed_args.config['refresh_token'],
        access_token=parsed_args.config['access_token'],
    ) as client:
        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover(parsed_args.config.get('custom_report'))
        elif parsed_args.catalog:
            sync(
                client=client,
                config=parsed_args.config,
                catalog=parsed_args.catalog,
                state=state
            )


if __name__ == '__main__':
    main()
