import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST, \
    CREATED_DATE_FIELD, CUSTOMER_KEY_FIELD, OBJECT_ID_FIELD, \
    MODIFIED_DATE_FIELD, with_properties
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table

LOGGER = singer.get_logger()

class ET_LinkSend(FuelSDK.ET_CUDSupport):
    def __init__(self):
        super(ET_LinkSend, self).__init__()
        self.obj_type = 'LinkSend'

class LinkSendDataAccessObject(DataAccessObject):
    REPLICATION_METHOD = "FULL_TABLE"
    SCHEMA = with_properties({
        'ID': ID_FIELD,
        'SendID': {
            'type': ['null', 'integer'],
            'description': 'Contains identifier for a specific send.',
        },
        'ClientID': {
            'type': ['null', 'integer'],
            'description': 'Contains identifier for a specific client.',
        },
        'PartnerKey': {
            'type': ['null', 'integer'],
            'description': '',
        },
        'TotalClicks': {
            'type': ['null', 'integer'],
            'description': '',
        },
        'UniqueClicks': {
            'type': ['null', 'integer'],
            'description': '',
        },
        'URL': {
            'type': ['null', 'string'],
            'description': '',
        },
        'Alias': {
            'type': ['null', 'string'],
            'description': '',
        },
        'LinkID': {
            'type': ['null', 'integer'],
            'description': 'Contains identifier for a specific link.',
        }
    })

    TABLE = 'link_send'
    KEY_PROPERTIES = ['SendID']

    def parse_object(self, obj):
        to_return = obj.copy()
        to_return['ClientID'] = to_return.get('Client', {}).get('ID')
        to_return['LinkID'] = to_return.get('Link', {}).get('ID')
        to_return['Alias'] = to_return.get('Link', {}).get('Alias')
        to_return['URL'] = to_return.get('Link', {}).get('URL')
        to_return['TotalClicks'] = to_return.get('Link', {}).get('TotalClicks')
        to_return['UniqueClicks'] = to_return.get('Link', {}).get('UniqueClicks')

        return super(LinkSendDataAccessObject, self).parse_object(to_return)

    def sync_data(self):
        pass

    def sync_data_by_sendID(self, sendId):
        if not sendId:
            return

        table = self.__class__.TABLE
        _filter = {}

        if sendId:
            _filter = {
                'Property': 'SendID',
                'SimpleOperator': 'equals',
                'Value': sendId
            }
        else:
            LOGGER.info('No send id here, moving on')
            return

        stream = request(
            self.__class__.TABLE, ET_LinkSend, self.auth_stub, _filter)
        for link_send in stream:
            link_send = self.filter_keys_and_parse(link_send)
            singer.write_records(table, [link_send])