import hashlib
import FuelSDK
import copy
import singer

from funcy import set_in, update_in, merge
from urllib.parse import urlencode
import requests
import backoff
from tap_exacttarget.client import request
from tap_exacttarget.dao import (DataAccessObject)
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table
from tap_exacttarget.util import sudsobj_to_dict


LOGGER = singer.get_logger()  # noqa


def _merge_in(collection, path, new_item):
    return update_in(collection, path, lambda x: merge(x, [new_item]))


def _convert_extension_datatype(datatype):
    if datatype == 'Boolean':
        return 'boolean'
    elif datatype == 'Decimal':
        return 'number'
    elif datatype == 'Number':
        return 'integer'

    return 'string'


def _get_tap_stream_id(extension):
    extension_name = extension.CustomerKey
    return 'data_extension.{}'.format(extension_name)


def _get_extension_name_from_tap_stream_id(tap_stream_id):
    return tap_stream_id.split('.')[1]


class DataExtensionDataAccessObject(DataAccessObject):

    @classmethod
    def matches_catalog(cls, catalog):
        return 'data_extension.' in catalog.get('stream')

    def _get_extensions(self):
        result = request(
            'DataExtension',
            FuelSDK.ET_DataExtension,
            self.auth_stub,
            props=['CustomerKey', 'Name'],
            batch_size=int(self.config.get('batch_size', 2500))
        )

        to_return = {}

        for extension in result:
            extension_name = str(extension.Name)
            customer_key = str(extension.CustomerKey)

            to_return[customer_key] = {
                'tap_stream_id': 'data_extension.{}'.format(customer_key),
                'stream': 'data_extension.{}'.format(extension_name),
                'replication_method': self.get_replication_method(),
                'key_properties': ['_CustomObjectKey'],
                'schema': {
                    'type': 'object',
                    'properties': {
                        '_CustomObjectKey': {
                            'type': ['null', 'string'],
                            'description': ('Hidden auto-incrementing primary '
                                            'key for data extension rows.'),
                        },
                        'CategoryID': {
                            'type': ['null', 'integer'],
                            'description': ('Specifies the identifier of the '
                                            'folder. (Taken from the parent '
                                            'data extension.)')
                        }
                    }
                },
                'metadata': [{'breadcrumb': (), 'metadata': {
                    'inclusion':'available'}},
                             {'breadcrumb': ('properties', '_CustomObjectKey'),
                              'metadata': {'inclusion':'available'}},
                             {'breadcrumb': ('properties', 'CategoryID'),
                              'metadata': {'inclusion':'available'}}]
            }
        return to_return

    def _get_fields(self, extensions):
        to_return = extensions.copy()

        result = request(
            'DataExtensionField',
            FuelSDK.ET_DataExtension_Column,
            self.auth_stub)

        for field in result:
            extension_id = field.DataExtension.CustomerKey
            field = sudsobj_to_dict(field)
            field_name = field['Name']

            if field.get('IsPrimaryKey'):
                to_return = _merge_in(
                    to_return,
                    [extension_id, 'key_properties'],
                    field_name)

            field_schema = {
                'type': [
                    'null',
                    _convert_extension_datatype(str(field.get('FieldType')))
                ],
                'description': str(field.get('Description')),
            }

            to_return = set_in(
                to_return,
                [extension_id, 'schema', 'properties', field_name],
                field_schema)

            # These fields are defaulted into the schema, do not add to metadata again.
            if field_name not in {'_CustomObjectKey', 'CategoryID'}:
                to_return[extension_id]['metadata'].append({
                    'breadcrumb': ('properties', field_name),
                    'metadata': {'inclusion': 'available'}
                })

        return to_return

    def generate_catalog(self):
        # get all the data extensions by requesting all the fields
        extensions_catalog = self._get_extensions()

        extensions_catalog_with_fields = self._get_fields(extensions_catalog)

        return extensions_catalog_with_fields.values()

    def parse_object(self, obj):
        properties = obj.get('Properties', {}).get('Property', {})
        to_return = {}

        for prop in properties:
            to_return[prop['Name']] = prop['Value']

        return to_return

    def filter_keys_and_parse(self, obj):
        row = obj
        to_return = {}

        obj_schema = self.catalog['schema']['properties']
        keys_match = {key.lower(): key for key in obj_schema.keys()}
        
        for k, v in row.items():
            field_schema = obj_schema.get(keys_match[k], {})
            to_return[keys_match[k]] = v
            # sometimes data extension fields have type integer or
            # number, but come back as strings from the API. we need
            # to explicitly cast them.
            if v is None:
                pass

            elif 'integer' in field_schema.get('type'):
                to_return[keys_match[k]] = int(v) if v else 0

            elif 'number' in field_schema.get('type'):
                to_return[keys_match[k]] = float(v) if v else 0.0

            elif ('boolean' in field_schema.get('type') and
                  isinstance(to_return[keys_match[k]], str)):
                # Extension bools can come through as a number of values, see:
                # https://help.salesforce.com/articleView?id=mc_es_data_extension_data_types.htm&type=5
                # In practice, looks like they come through as either "True"
                # or "False", but for completeness I have included the other
                # possible values.
                if str(to_return[keys_match[k]]).lower() in [1, "1", "y", "yes", "true"]:
                    to_return[keys_match[k]] = True
                elif str(to_return[keys_match[k]]).lower() in [0, "0", "n", "no", "false"]:
                    to_return[keys_match[k]] = False
                else:
                    to_return[keys_match[k]] = None

            if v and keys_match[k] in self.config.get('sensitive_fields', '').replace(" ", "").split(','):
                to_return[keys_match[k]] = hashlib.md5(v.encode('utf-8')).hexdigest()

        return to_return

    def sync_data(self):
        LOGGER.info('...............START SYNC.')
        tap_stream_id = self.catalog.get('tap_stream_id')
        table = self.catalog.get('stream')
        (_, customer_key) = tap_stream_id.split('.', 1)

        keys = self.get_catalog_keys()

        keys.remove('CategoryID')

        replication_key = None

        start = get_last_record_value_for_table(
            self.state,
            table,
            self.config.get('start_date'),
            self.config.get('offset_start_date', None),
            self.is_full_table_mode()
        )

        if start is None:
            start = self.config.get('start_date')

        for key in ['ModifiedDate', 'JoinDate', 'CreatedDate']:
            if key in keys:
                LOGGER.info(f'replication key = {key}')
                replication_key = key
       
        parent_result = None
        parent_extension = None
        parent_result = request(
            'DataExtension',
            FuelSDK.ET_DataExtension,
            self.auth_stub,
            search_filter={
                'Property': 'CustomerKey',
                'SimpleOperator': 'equals',
                'Value': customer_key,
            },
            props=['CustomerKey', 'CategoryID'])

        parent_extension = next(parent_result)
        parent_category_id = parent_extension.CategoryID

        bookmark = self.state.get('bookmarks', {}).get(table, {})

        page = 1 if bookmark and bookmark.get('is_completed', False) else (bookmark.get('page') + 1 if bookmark.get('page') else 1)

        # restart from previous run if incremental mode and have the bookmark is completed to false
        if self.get_replication_method() == 'INCREMENTAL' and not bookmark.get('is_completed', False) and bookmark.get('page'):
            LOGGER.info(f'...............Previous run was incompleted: {page}')
            start = bookmark.get('previous_start_date', start)

        if self.get_replication_method() == 'FULL_TABLE':
            #if full table restart from the beginning
            page = 1

        LOGGER.info(f'...............Start from: {page}')


        keys.remove('_CustomObjectKey')
        response = self.request_data_extension_via_rest_api(page, customer_key, keys, replication_key, start)

        resp = response.json()
            
        self.write_records_from_rest_api(resp.get('items', []), table, replication_key, parent_category_id)

        # save state after each upload to unable rerun
        self.save_last_page_sync(table, page, start)

        # if more results
        while resp.get('links', []) and resp.get('links').get('next'):
            page += 1
            response = self.request_data_extension_via_rest_api(page, customer_key, keys, replication_key, start)

            resp = response.json()

            self.write_records_from_rest_api(resp.get('items', []), table, replication_key, parent_category_id)

            # save state after each upload to unable rerun
            self.save_last_page_sync(table, page, start)

        self.state['bookmarks'][table]['is_completed'] = True

        LOGGER.info('...............END SYNC.')

    @backoff.on_exception(backoff.expo, (requests.exceptions.HTTPError), max_tries=5)
    def request_data_extension_via_rest_api(self, page, customer_key, keys, replication_key, start_date):
        LOGGER.info(f'...............REST request for data extension. parameters: page: {page}, pagesize: 2500, data_extension: {customer_key}')
        params = urlencode({
            "$page": page,
            "$pagesize": 2500,
            "$fields": ','.join(keys)
        })
        if replication_key:
            params += f"&$filter={replication_key}%20gt%20'{start_date}'"
        LOGGER.info(f"'...............params: {params}")

        et_subdomain = self.config.get('tenant_subdomain')
        access_token = self.auth_stub.authToken
        
        response = requests.get(f'https://{et_subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/{customer_key}/rowset?{params}', headers={
            'Authorization': f'Bearer {access_token}'
        })

        resp = response.json()

        if response.status_code != 200:
            message_error = resp.get('message')
            error_code = resp.get('errorcode')
            LOGGER.error(f'RequestApiError : {message_error}, errorcode: {error_code}')
            if response.status_code == 401:
                # if not authorized try to refresh token
                self.auth_stub.refresh_token_with_oAuth2(True)
            response.raise_for_status()

        return response

    def write_records_from_rest_api(self, items, table, replication_key, parent_category_id):
        catalog_copy = copy.deepcopy(self.catalog)
        for row in items:
            raw_data = dict()
            raw_data.update(row.get('keys', {}))
            raw_data.update(row.get('values', {}))

            row = self.filter_keys_and_parse(raw_data)
            row['_CustomObjectKey'] = None
            row['CategoryID'] = parent_category_id

            self.write_records_with_transform(row, catalog_copy, table)

            self.state = incorporate(self.state,
                    table,
                    replication_key,
                    row.get(replication_key))

        LOGGER.info(f'...............Write {len(items)} records')

    def save_last_page_sync(self, table, page, start):
        new_state = self.state.copy()

        if 'bookmarks' not in new_state:
            new_state['bookmarks'] = {}

        new_state['bookmarks'][table] = {
            **new_state['bookmarks'].get(table, {}),
            'previous_start_date': start,
            'is_completed': False,
            'page': page
        }

        self.state = new_state
        save_state(self.state)