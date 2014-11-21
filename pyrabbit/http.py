
import json
import os
import socket
import httplib2
try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin

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
    A wrapper for (currently) httplib2. Abstracts away
    things like path building, return value parsing, etc.,
    so the api module code stays clean and easy to read/use.

    """

    def __init__(self, server, uname, passwd, timeout=5):
        """
        :param string server: 'host:port' string denoting the location of the
            broker and the port for interfacing with its REST API.
        :param string uname: Username credential used to authenticate.
        :param string passwd: Password used to authenticate w/ REST API
        :param int timeout: Integer number of seconds to wait for each call.

        """

        self.client = httplib2.Http(timeout=timeout)
        self.client.add_credentials(uname, passwd)
        self.base_url = 'http://%s/api/' % server

    def decode_json_content(self, content):
        """
        Returns the JSON-decoded Python representation of 'content'.

        :param json content: A Python JSON object.

        """
        try:
            py_ct = json.loads(content)
        except ValueError as out:
            # If there's a 404 or other error, the response will not be JSON.
            return None
        except TypeError:
            # in later Python 3.x versions, some calls return bytes objects.
            py_ct = json.loads(content.decode())
        return py_ct

    def do_call(self, path, reqtype, body=None, headers=None):
        """
        Send an HTTP request to the REST API.

        :param string path: A URL
        :param string reqtype: The HTTP method (GET, POST, etc.) to use
            in the request.
        :param string body: A string representing any data to be sent in the
            body of the HTTP request.
        :param dictionary headers:
            "{header-name: header-value}" dictionary.

        """
        url = urljoin(self.base_url, path)
        try:
            resp, content = self.client.request(url,
                                                reqtype,
                                                body,
                                                headers)
        except socket.timeout as out:
            raise NetworkError("Timout while trying to connect to RabbitMQ")
        except Exception as out:
            # net-related exception types from httplib2 are unpredictable.
            raise NetworkError("Error: %s %s" % (type(out), out))

        # RabbitMQ will return content even on certain failures.
        if content:
            content = self.decode_json_content(content)

        # 'success' HTTP status codes are 200-206
        if resp.status < 200 or resp.status > 206:
            raise HTTPError(content, resp.status, resp.reason, path, body)
        else:
            if content:
                return content
            else:
                return None
