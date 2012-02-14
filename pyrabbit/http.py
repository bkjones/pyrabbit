
import httplib2
import json
import os

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

        # Actual, useful reason for failure returned by RabbitMQ
        self.detail=None
        if content.get('reason'):
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

    def __init__(self, server, uname, passwd):
        """
        :param string server: 'host:port' string denoting the location of the
            broker and the port for interfacing with its REST API.
        :param string uname: Username credential used to authenticate.
        :param string passwd: Password used to authenticate w/ REST API

        """

        self.client = httplib2.Http()
        self.client.add_credentials(uname, passwd)
        self.base_url = 'http://%s/api' % server

    def decode_json_content(self, content):
        """
        Returns the JSON-decoded Python representation of 'content'.

        :param json content: A Python JSON object.

        """
        str_ct = content.decode('utf8')
        try:
            py_ct = json.loads(str_ct)
        except ValueError as out:
            print("%s - (%s) (%s)" % (out, str_ct, type(str_ct)))
            return None
        return py_ct

    def do_call(self, path, reqtype, body=None, headers=None):
        """
        Send an HTTP request to the REST API.

        :param string path: A URL
        :param string reqtype: The HTTP method (GET, POST, etc.) to use
            in the request.
        :param string body: A string representing any data to be sent in the
            body of the HTTP request.
        :param dict headers: {header-name: header-value} dictionary.

        """
        url = os.path.join(self.base_url, path)
        try:
            resp, content = self.client.request(url,
                                                reqtype,
                                                body,
                                                headers)
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
                return True
