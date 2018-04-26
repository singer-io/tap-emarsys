import os
import hashlib
from datetime import datetime
from binascii import hexlify
from base64 import b64encode

import requests
import backoff
from singer import metrics

class RateLimitException(Exception):
    pass

class Client(object):
    BASE_URL = 'http://api.emarsys.net/api/v2'

    def __init__(self, config):
        self.user_agent = config.get('user_agent')
        self.session = requests.Session()
        self.username = config.get('username')
        self.secret = config.get('secret')

    def get_wsse_header(self):
        nonce = hexlify(os.urandom(16)).decode('utf-8')
        created = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+00:00')
        sha1 = hashlib.sha1(str.encode(nonce + created + self.secret)).hexdigest()
        password_digest = bytes.decode(b64encode(str.encode(sha1)))

        return ('UsernameToken Username="{}", ' +
                'PasswordDigest="{}", Nonce="{}", Created="{}"').format(
                    self.username,
                    password_digest,
                    nonce,
                    created)

    def url(self, path):
        return self.BASE_URL + path

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request(self, tap_stream_id, method, path, **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        if self.user_agent:
            kwargs['headers']['User-Agent'] = self.user_agent
        kwargs['headers']['X-WSSE'] = self.get_wsse_header()
        print(kwargs)

        with metrics.http_request_timer(tap_stream_id) as timer:
            response = requests.request(method, self.url(path), **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        ## TODO: check replyCode?
        if response.status_code in [429, 503]:
            raise RateLimitException()
        response.raise_for_status()
        return response.json()['data']

    def get(self, tap_stream_id, path, **kwargs):
        return self.request(tap_stream_id, 'get', path, **kwargs)
