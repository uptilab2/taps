from dateutil.parser import parse

import datetime
import singer

from voluptuous import Schema, Required, Optional

from tap_exacttarget.pagination import DATE_FORMAT

LOGGER = singer.get_logger()

STATE_SCHEMA = Schema({
    Required('bookmarks'): {
        str: {
            Optional('last_record'): str,
            Optional('previous_start_date'): str,
            Optional('page'): int,
            Optional('is_completed'): bool,
            Optional('field'): str,
        }
    }
})


def get_last_record_value_for_table(state, table, start_date_default, offset_start_date=None, is_full_table_mode=False):
    raw = state.get('bookmarks', {}) \
               .get(table, {}) \
               .get('last_record')

    if raw is None:
        date_obj = datetime.datetime.strptime(start_date_default, DATE_FORMAT)
        LOGGER.info(f'====== last record not found, use start date: {date_obj} ======')
    else:
        starting_date = start_date_default if is_full_table_mode and not offset_start_date else raw
        date_obj = datetime.datetime.strptime(starting_date, DATE_FORMAT)
        if is_full_table_mode:
            LOGGER.info(f'====== full table mode date use: {date_obj} ======')
        else:
            LOGGER.info(f'====== last recorded value date is: {date_obj} ======')

        if offset_start_date:
            date_obj = date_obj - datetime.timedelta(days=int(offset_start_date))
            LOGGER.info(f'====== offset start date found: {offset_start_date}, new start date: {date_obj} ======')

    return date_obj.strftime(DATE_FORMAT)


def incorporate(state, table, field, value):
    if value is None:
        return state

    new_state = state.copy()

    parsed = parse(value).strftime("%Y-%m-%dT%H:%M:%SZ")

    if 'bookmarks' not in new_state:
        new_state['bookmarks'] = {}

    if(new_state['bookmarks'].get(table, {}).get('last_record') is None or
       new_state['bookmarks'].get(table, {}).get('last_record') < parsed):
        new_state['bookmarks'][table] = {
            'field': field,
            'last_record': parsed,
        }

    return new_state


def save_state(state):
    if not state:
        return

    STATE_SCHEMA(state)

    # LOGGER.info('Updating state.')

    singer.write_state(state)
