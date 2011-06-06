
import httplib2
import json
import os

class HTTPError(Exception):
    """
    An error response from the API server. This should be an
    HTTP error of some kind (404, 500, etc).

    """
    def __init__(self, status=None, reason=None):
        self.status = status
        self.reason = reason
        self.output = "%s - %s" % (self.status, self.reason)

    def __str__(self):
        return self.output

class NetworkError(Exception):
    pass

class APIError(Exception):
    pass

class HTTPClient(object):
    urls = {'overview': 'overview',
            'all_queues': 'queues',
            'all_exchanges': 'exchanges',
            'all_channels': 'channels',
            'all_connections': 'connections',
            'all_nodes': 'nodes',
            'all_vhosts': 'vhosts',
            'all_users': 'users',
            'whoami': 'whoami',
            'queues_by_vhost': 'queues/%s',
            'queues_by_name': 'queues/%s/%s',
            'exchanges_by_vhost': 'exchanges/%s',
            'exchange_by_name': 'exchanges/%s/%s',
            'live_test': 'aliveness-test/%s',
            'purge_queue': 'queues/%s/%s/contents',
            'connections_by_name': 'connections/%s'}

    def __init__(self, server, uname, passwd):
        self.client = httplib2.Http()
        self.client.add_credentials(uname, passwd)
        self.base_url = 'http://%s/api' % server

    def decode_json_content(self, content):
        str_ct = content.decode('utf8')
        py_ct = json.loads(str_ct)
        return py_ct

    def do_call(self, path, reqtype):
        try:
            resp, content = self.client.request(path, reqtype)
        except Exception as out:
            # net-related exception types from httplib2 are unpredictable.
            raise NetworkError("Error: %s %s" % (type(out), out))

        if resp.status != 200:
            raise HTTPError(resp.status, resp.reason)
        else:
            return resp, content

    def is_alive(self, vhost='%2F'):
        """
        Uses the aliveness-test API call to determine if the
        server is alive and the vhost is active. The broker (not this code)
        creates a queue and then sends/consumes a message from it.

        """
        uri = os.path.join(self.base_url,
                           HTTPClient.urls['live_test'] % vhost)

        try:
            resp, content = self.do_call(uri, 'GET')
        except HTTPError as e:
            if e.status == 404:
                raise APIError("No vhost named '%s'" % vhost)
            raise

        if resp.status == 200:
            return True
        else:
            return False

    def get_whoami(self):
        path = os.path.join(self.base_url, HTTPClient.urls['whoami'])
        resp, content = self.do_call(path, 'GET')
        whoami = self.decode_json_content(content)
        return whoami

    def get_users(self):
        path = os.path.join(self.base_url, HTTPClient.urls['all_users'])

        try:
            resp, content = self.do_call(path, 'GET')
        except HTTPError as e:
            if e.status == 401:
                raise APIError("Only admin can access user data")
            else:
                raise

        users = self.decode_json_content(content)
        return users

    def get_overview(self):
        """
        /api/overview provides a bunch of miscellaneous data about the
        broker instance itself.

        """
        resp, content = self.do_call(os.path.join(self.base_url,
                                            HTTPClient.urls['overview']),
                                            'GET')

        overview_data = self.decode_json_content(content)
        return overview_data

    def get_all_vhosts(self):
        """
        Get a list of all vhosts a broker knows about.

        """
        resp, content = self.do_call(os.path.join(self.base_url,
                                            HTTPClient.urls['all_vhosts']),
                                            'GET')

        vhost_data = self.decode_json_content(content)
        return vhost_data

    def get_queues(self, vhost=None):
        """
        Get a list of all queues a broker knows about, or all queues in an
        optionally-supplied vhost name.

        """
        if vhost:
            path = HTTPClient.urls['queues_by_vhost'] % vhost
        else:
            path = HTTPClient.urls['all_queues']

        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        queues = self.decode_json_content(content)
        return queues

    def get_queue(self, vhost, name):
        path = HTTPClient.urls['queues_by_name'] % (vhost, name)
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        queue = self.decode_json_content(content)
        return queue


    def purge_queue(self, vhost, name):
        """
        The queues parameter should be a list of one or more queue objects.

        """
        path = HTTPClient.urls['purge_queue'] % (vhost, name)
        try:
            self.do_call(os.path.join(self.base_url, path), 'DELETE')
        except HTTPError as e:
            if e.status == 204:
                return True
            else:
                raise
        return True

    def get_exchanges(self, vhost=None):
        if vhost:
            path = HTTPClient.urls['exchanges_by_vhost'] % vhost
        else:
            path = HTTPClient.urls['all_exchanges']

        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        exchanges = self.decode_json_content(content)
        return exchanges

    def get_exchange(self, vhost, name):
        """
        Gets a single exchange which requires a vhost and name.

        """
        path = HTTPClient.urls['exchange_by_name'] % (vhost, name)
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        exch = self.decode_json_content(content)
        return exch

    def get_connections(self, name=None):
        if name:
            path = HTTPClient.urls['connections_by_name']
        else:
            path = HTTPClient.urls['all_connections']
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        conns = self.decode_json_content(content)
        return conns

    def get_channels(self):
        path = HTTPClient.urls['all_channels']
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        chans = self.decode_json_content(content)
        return chans
