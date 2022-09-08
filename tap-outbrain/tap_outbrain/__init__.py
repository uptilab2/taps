#!/usr/bin/env python3

from decimal import Decimal

import argparse
import base64
import copy
import datetime
import json
import os
import sys
import time
import dateutil.parser

import backoff
import requests
import singer
import singer.requests
from singer import utils

import tap_outbrain.schemas as schemas

LOGGER = singer.get_logger()
SESSION = requests.Session()

BASE_URL = 'https://api.outbrain.com/amplify/v0.1'
CONFIG = {}

DEFAULT_STATE = {
    "campaigns": {}
}

DEFAULT_START_DATE = '2016-08-01'

# We can retrieve at most 2 campaigns per minute. We only have 5.5 hours
# to run so that works out to about 660 (120 campaigns per hour * 5.5 =
# 660) campaigns.
TAP_CAMPAIGN_COUNT_ERROR_CEILING = 660
MARKETERS_CAMPAIGNS_MAX_LIMIT = 50
# This is an arbitrary limit and can be tuned later down the road if we
# see need for it. (Tested with 200 at least)
REPORTS_MARKETERS_PERIODIC_MAX_LIMIT = 500


@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException),
                      jitter=backoff.random_jitter,
                      max_tries=5,
                      giveup=singer.requests.giveup_on_http_4xx_except_429)
def request(url, access_token, params):
    LOGGER.info("Making request: GET {} {}".format(url, params))
    headers = {'OB-TOKEN-V1': access_token}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request('GET', url, headers=headers, params=params).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    if resp.status_code == 429:
        limit_left = int(resp.headers.get('rate-limit-msec-left', 60)) / 1000
        LOGGER.info(
            'Limit is exceeded. Sleeping {} sec '
            'before making the next reporting request.'
            .format(limit_left))
        time.sleep(limit_left)

    if resp.status_code >= 400:
        LOGGER.error("GET {} [{} - {}]".format(req.url, resp.status_code, resp.content))
        resp.raise_for_status()

    return resp


def generate_token(username, password):
    LOGGER.info("Generating new token using basic auth.")

    auth = requests.auth.HTTPBasicAuth(username, password)
    response = requests.get('{}/login'.format(BASE_URL), auth=auth)
    LOGGER.info("Got response code: {}".format(response.status_code))
    response.raise_for_status()

    return response.json().get('OB-TOKEN-V1')


def parse_datetime(date_time):
    parsed_datetime = dateutil.parser.parse(date_time)

    # the assumption is that the timestamp comes in in UTC
    return parsed_datetime.isoformat('T') + 'Z'


def parse_performance(result, extra_fields):
    metrics = result.get('metrics', {})
    metadata = result.get('metadata', {})
    to_return = {}
    to_return.update(extra_fields)
    to_return.update({
        'date': metadata.get('fromDate'),
        'impressions': int(metrics.get('impressions', 0)),
        'clicks': int(metrics.get('clicks', 0)),
        'ctr': float(metrics.get('ctr', 0.0)),
        'spend': float(metrics.get('spend', 0.0)),
        'ecpc': float(metrics.get('ecpc', 0.0)),
        'conversions': int(metrics.get('conversions', 0)),
        'conversionRate': float(metrics.get('conversionRate', 0.0)),
        'cpa': float(metrics.get('cpa', 0.0)),
    })
    return to_return


def get_date_ranges(start, end, interval_in_days):
    if start > end:
        return []

    to_return = []
    interval_start = start

    while interval_start < end:
        to_return.append({
            'from_date': interval_start,
            'to_date': min(end,
                           (interval_start + datetime.timedelta(
                               days=interval_in_days-1)))
        })

        interval_start = interval_start + datetime.timedelta(
            days=interval_in_days)
    return to_return


def sync_campaigns_with_performance(state, access_token, account_id, campaign, is_full_table, add_retrieve_date):
    """
    This function is heavily parameterized as it is used to sync performance
    both based on campaign ID alone, and by campaign ID and link ID.

    - `state`: state map
    - `access_token`: access token for Outbrain Amplify API
    - `account_id`: Outbrain marketer ID
    - `table_name`: the table name to use. At present:
      `campaign_performance`
    - `state_sub_id`: the id to use within the state map to identify this
                      sub-object. For example,

                        state['campaign_performance'][state_sub_id]

                      is used for the `campaign_performance` table.
    - `extra_params`: extra params sent to the Outbrain API
    - `extra_persist_fields`: extra fields pushed into the destination data.
                              For example:

                                {'campaignId': '000b...'}
    """
    table_name = 'campaigns'
    state_sub_id = campaign.get('id')

    # sync 2 days before last saved date, or DEFAULT_START_DATE
    start_date = state.get(table_name, {}).get(state_sub_id, DEFAULT_START_DATE)

    if is_full_table:
        start_date = DEFAULT_START_DATE
    
    from_date = datetime.datetime.strptime(
        start_date,
        '%Y-%m-%d').date() - datetime.timedelta(days=2)

    to_date = datetime.date.today()

    interval_in_days = REPORTS_MARKETERS_PERIODIC_MAX_LIMIT

    date_ranges = get_date_ranges(from_date, to_date, interval_in_days)
    last_request_start = None

    for date_range in date_ranges:
        LOGGER.info(
            'Pulling {} for {} from {} to {}'
            .format(table_name,
                    {'id': campaign.get('id')},
                    date_range.get('from_date'),
                    date_range.get('to_date')))

        params = {
            'from': date_range.get('from_date'),
            'to': date_range.get('to_date'),
            'breakdown': 'daily',
            'limit': REPORTS_MARKETERS_PERIODIC_MAX_LIMIT,
            'sort': '+fromDate',
            'includeArchivedCampaigns': True,
            'campaignId': campaign.get('id')
        }

        last_request_start = utils.now()
        response = request(
            '{}/reports/marketers/{}/periodic'.format(BASE_URL, account_id),
            access_token,
            params).json()
        if REPORTS_MARKETERS_PERIODIC_MAX_LIMIT < response.get('totalResults'):
            LOGGER.warn('More performance data (`{}`) than the tap can currently retrieve (`{}`)'.format(
                response.get('totalResults'), REPORTS_MARKETERS_PERIODIC_MAX_LIMIT))
        else:
            LOGGER.info('Syncing `{}` rows of performance data for campaign `{}`. Requested `{}`.'.format(
                response.get('totalResults'), state_sub_id, REPORTS_MARKETERS_PERIODIC_MAX_LIMIT))
        last_request_end = utils.now()

        LOGGER.info('Done in {} sec'.format(
            last_request_end.timestamp() - last_request_start.timestamp()))

        if add_retrieve_date:
            campaign['retrieve_date'] = datetime.datetime.now().strftime("%Y-%m-%d")

        performance = [
            parse_performance(result, campaign)
            for result in response.get('results')]

        for record in performance:
            singer.write_record(table_name, record, time_extracted=last_request_end)

        last_record = performance[-1]
        new_from_date = last_record.get('date')

        state[table_name][state_sub_id] = new_from_date
        singer.write_state(state)

        from_date = new_from_date


def parse_campaign(campaign):
    if campaign.get('budget') is not None:
        campaign['budget'] = {key: value for key, value in campaign['budget'].items() if key in schemas.campaign['properties']['budget']['properties'] }
        campaign['budget']['creationTime'] = parse_datetime(
            campaign.get('budget').get('creationTime'))
        campaign['budget']['lastModified'] = parse_datetime(
            campaign.get('budget').get('lastModified'))

    if campaign.get('liveStatus') is not None:
        campaign['onAirReason'] = campaign['liveStatus']['onAirReason']
        campaign['campaignOnAir'] = campaign['liveStatus']['campaignOnAir']

    campaign = {key: value for key, value in campaign.items() if key in schemas.campaign['properties'] }

    return campaign


def get_campaigns_page(account_id, access_token, offset):
    # NOTE: We probably should be more aggressive about ensuring that the
    # response was successful.
    return request(
        '{}/marketers/{}/campaigns'.format(BASE_URL, account_id),
        access_token, {'limit': MARKETERS_CAMPAIGNS_MAX_LIMIT,
                       'offset': offset}).json()


def get_campaign_pages(account_id, access_token):
    more_campaigns = True
    offset = 0

    while more_campaigns:
        LOGGER.info('Retrieving campaigns from offset `{}`'.format(
            offset))
        campaign_page = get_campaigns_page(account_id, access_token,
                                           offset)
        if TAP_CAMPAIGN_COUNT_ERROR_CEILING < campaign_page.get('totalCount'):
            msg = 'Tap found `{}` campaigns which is more than can be retrieved in the alloted time (`{}`).'.format(
                campaign_page.get('totalCount'), TAP_CAMPAIGN_COUNT_ERROR_CEILING)
            LOGGER.error(msg)
            raise Exception(msg)
        LOGGER.info('Retrieved offset `{}` campaigns out of `{}`'.format(
            offset, campaign_page.get('totalCount')))
        yield campaign_page
        if (offset + MARKETERS_CAMPAIGNS_MAX_LIMIT) < campaign_page.get('totalCount'):
            offset += MARKETERS_CAMPAIGNS_MAX_LIMIT
        else:
            more_campaigns = False

    LOGGER.info('Finished retrieving `{}` campaigns'.format(
        campaign_page.get('totalCount')))


def sync_campaign_page(state, access_token, account_id, campaign_page, is_full_table, add_retrieve_date):
    campaigns = [parse_campaign(campaign) for campaign
                 in campaign_page.get('campaigns', [])]

    for campaign in campaigns:
        sync_campaigns_with_performance(state, access_token, account_id,
                                  campaign, is_full_table, add_retrieve_date)


def sync_campaigns(state, access_token, account_id, is_full_table, add_retrieve_date):
    LOGGER.info('Syncing campaigns.')

    for campaign_page in get_campaign_pages(account_id, access_token):
        sync_campaign_page(state, access_token, account_id, campaign_page, is_full_table, add_retrieve_date)

    LOGGER.info('Done!')


def do_sync(args):
    #pylint: disable=global-statement
    global DEFAULT_START_DATE
    state = DEFAULT_STATE
    is_full_table = False
    add_retrieve_date = False

    with open(args.config) as config_file:
        config = json.load(config_file)
        CONFIG.update(config)

    missing_keys = []
    if 'username' not in config:
        missing_keys.append('username')
    else:
        username = config['username']

    if 'password' not in config:
        missing_keys.append('password')
    else:
        password = config['password']

    if 'account_id' not in config:
        missing_keys.append('account_id')
    else:
        account_id = config['account_id']

    if 'start_date' not in config:
        missing_keys.append('start_date')
    else:
        # only want the date
        DEFAULT_START_DATE = config['start_date'][:10]

    if missing_keys:
        LOGGER.fatal("Missing {}.".format(", ".join(missing_keys)))
        raise RuntimeError

    access_token = config.get('access_token', None)

    if not access_token:
        access_token = generate_token(username, password)

    if access_token is None:
        LOGGER.fatal("Failed to generate a new access token.")
        raise RuntimeError

    if 'replication_method' in config:
        is_full_table = True if config['replication_method'] == 'FULL_TABLE' else False

    if 'add_retrieve_date' in config:
        add_retrieve_date = True if config['add_retrieve_date'] else False

    # NEVER RAISE THIS ABOVE DEBUG!
    LOGGER.debug('Using access token `{}`'.format(access_token))
    if add_retrieve_date:
        schemas.campaign['properties']['retrieve_date'] = {
            'type': 'string',
            'format': 'date',
            'description': 'The retrieve date of data',
        }
    singer.write_schema('campaigns',
                        schemas.campaign,
                        key_properties=["id", "date"],
                        bookmark_properties=["date"])

    sync_campaigns(state, access_token, account_id, is_full_table, add_retrieve_date)


def main_impl():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    do_sync(args)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
