import json
import os.path
import re
from tap_toast.context import Context
from jsonpath_ng import parse
from tap_toast.utils import get_abs_path
from base64 import b64encode
import singer
import requests
import urllib.parse


logger = singer.get_logger()


def setVars(string):
    for group in re.findall(r'{{([a-zA-Z_-]*)}}', string):
        string = string.replace(f'{{{{{group}}}}}', Context.config[group])
    return string


def setOptionalVar(string, url_encode=False):
    for group in re.findall(r'{{([a-zA-Z_-]*)}}', string):
        if group in Context.config:
            text = urllib.parse.quote_plus(Context.config[group]) if url_encode else Context.config[group]
            string = string.replace(f'{{{{{group}}}}}', text)
        else:
            return False, string
    return True, string


def getHeaderFromBody(body):
    if body['mode'] == 'raw':
        if 'options' in body:
            if body['options']['raw']['language'] == 'json':
                return {'Content-Type': 'application/json'}


class Postman:
    events = []
    request = None
    authentication = None
    forced_url = None
    name = None
    postProcess = None
    preProcess = None

    def __init__(self, postname):
        self.name = postname['filename']
        filename = get_abs_path(f'postman/{self.name}.json', Context.config.get('base_path'))
        if not os.path.exists(filename):
            return
        logger.info(f'Read Postman from "{filename}"')
        file = json.load(open(filename))
        self.readItemConfig(file, postname['item'])
        if self.request is None:
            raise NameError(f'Item "{postname["item"]}" not found in postman file "{filename}"')
        self.authentication = None if 'auth' not in file else file['auth']['type']

    def readItemConfig(self, file, name):
        for item in file['item']:
            if item['name'] == name:
                self.request = item['request']
                if 'event' in item:
                    for event in item['event']:
                        if 'variable' in event:
                            self.events.append(event)

    @property
    def isAnonymous(self):
        return self.authentication is None

    @property
    def isValid(self):
        return self.request is not None

    @property
    def is_authorized(self):
        if self.isAnonymous:
            return True
        elif self.authentication == 'bearer':
            return 'bearer' in Context.config
        elif self.authentication == 'basic':
            return 'username' in Context.config and 'password' in Context.config

    def _authHeader(self):
        if self.authentication == 'bearer':
            return f'Bearer {Context.config["bearer"]}'
        elif self.authentication == 'basic':
            pwd = f'{Context.config["username"]}:{Context.config["password"]}'
            b64pwd = b64encode(str.encode(pwd))
            return f'Basic {b64pwd.decode()}'

    @property
    def url(self):
        if self.forced_url:
            return self.forced_url

        _url = self.request['url']
        res = _url['host'][0]
        if 'path' in _url:
            for p in _url['path']:
                res = res + f'/{p}'
        if 'query' in _url:
            qs = ''
            for q in _url['query']:
                var = setOptionalVar(f'&{q["key"]}={q["value"]}', True)
                if var[0]:
                    qs = qs + var[1]
            res = res + qs.replace('&', '?', 1)
        return setVars(res)

    def setUrl(self, url):
        self.forced_url = url

    @property
    def headers(self):
        headers = {}
        if not self.isAnonymous:
            headers.update({'Authorization': self._authHeader()})

        if 'header' in self.request:
            for header in self.request['header']:
                headers.update({header['key']: setVars(header['value'])})
        if 'body' in self.request:
            headers.update(getHeaderFromBody(self.request['body']))
        return headers

    @property
    def method(self):
        return self.request['method']

    @property
    def payload(self):
        if 'body' in self.request:
            if self.request['body']['mode'] == 'raw':
                return setVars(self.request['body']['raw'])

    def setToken(self, res):
        for event in self.events:
            if 'variable' in event:
                for var in event['variable']:
                    for key in var.keys():
                        expr = parse(var[key])
                        val = expr.find(res)
                        logger.info(f'Postman, setToken to var {key}')
                        Context.config[key] = val[0].value if res is not None else None

    def call(self):
        if self.preProcess:
            self.preProcess(self)
        payload = self.payload
        headers = self.headers
        url = self.url

        logger.info(f'Request {self.method} {url}')
        if self.method == "GET":
            response = requests.get(url, headers=headers)
        else:
            response = requests.post(url, headers=headers, json=payload)
        logger.info(f'{self.method} request {url} response {response.status_code}')
        response.raise_for_status()
        if self.postProcess:
            self.postProcess(self, response)

        try:
            res = response.json()
            if isinstance(res, dict):
                res = [res]
        except ValueError as err:
            logger.error(f'HTTP error: {response.reason}')
            raise err

        return res

    def get_authorization_token(self):
        payload = self.payload
        headers = self.headers
        url = self.url
        logger.info(f'POST authentication request {url}')
        response = requests.post(url, data=payload, headers=headers)
        logger.info(f'POST authentication request {url} response {response.status_code}')
        response.raise_for_status()
        res = response.json()
        self.setToken(res)
        logger.info('Authorization successful.')