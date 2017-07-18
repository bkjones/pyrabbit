
import json
import os
import socket
import requests
import requests.exceptions
from requests.auth import HTTPBasicAuth
try:
    from urlparse import urljoin, urlparse, urlunparse
except ImportError:
    from urllib.parse import urljoin, urlparse, urlunparse

class HTTPError(Exception):
    """
    An error response from the API server. This should be an
    HTTP error of some kind (404, 500, etc).

    """
    def __init__(self, content, status=None, reason=None, path=None, body=None):
        #HTTP status code
        self.status = status
        # human readable HTTP status
        self.reason = reason
        self.path = path
        self.body = body
        self.detail = None

        # Actual, useful reason for failure returned by RabbitMQ
        self.detail=None
        if content and content.get('reason'):
            self.detail = content['reason']

        self.output = "%s - %s (%s) (%s) (%s)" % (self.status,
                                             self.reason,
                                             self.detail,
                                             self.path,
                                             repr(self.body))

    def __str__(self):
        return self.output


class NetworkError(Exception):
    """Denotes a failure to communicate with the REST API

    """
    pass


class HTTPClient(object):
    """
    A wrapper for requests. Abstracts away
    things like path building, return value parsing, etc.,
    so the api module code stays clean and easy to read/use.

    """

    def __init__(self, api_url, uname, passwd, timeout=5, scheme='http'):
        """
        :param string api_url: The base URL for the broker API.
        :param string uname: Username credential used to authenticate.
        :param string passwd: Password used to authenticate w/ REST API
        :param int timeout: Integer number of seconds to wait for each call.
        :param string scheme: HTTP scheme used to connect

        """
        self.auth = HTTPBasicAuth(uname, passwd)
        self.timeout = timeout
        api_url = '%s://%s/api/' % (scheme, api_url)
        self.base_url = api_url

    def do_call(self, path, method, body=None, headers=None):
        """
        Send an HTTP request to the REST API.

        :param string path: A URL
        :param string method: The HTTP method (GET, POST, etc.) to use
            in the request.
        :param string body: A string representing any data to be sent in the
            body of the HTTP request.
        :param dictionary headers:
            "{header-name: header-value}" dictionary.

        """
        url = urljoin(self.base_url, path)
        try:
            resp = requests.request(method, url, data=body, headers=headers,
                                    auth=self.auth, timeout=self.timeout)
        except requests.exceptions.Timeout as out:
            raise NetworkError("Timeout while trying to connect to RabbitMQ")
        except requests.exceptions.RequestException as err:
            # All other requests exceptions inherit from RequestException
            raise NetworkError("Error during request %s %s" % (type(err), err))

        try:
            content = resp.json()
        except ValueError as out:
            content = None

        # 'success' HTTP status codes are 200-206
        if resp.status_code < 200 or resp.status_code > 206:
            raise HTTPError(content, resp.status_code, resp.text, path, body)
        else:
            if content:
                return content
            else:
                return resp.status_code
