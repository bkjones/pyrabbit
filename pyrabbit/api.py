import httplib2
import json
from collections import namedtuple
import os
try:
    # Python 3.x
    from urllib.parse import urlunparse
except ImportError:
    # Python 2.7
    from urlparse import urlunparse

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
    urls = {'overview': 'api/overview',
            'all_queues': 'api/queues',
            'all_exchanges': 'api/exchanges',
            'all_channels': 'api/channels',
            'all_connections': 'api/connections',
            'all_nodes': 'api/nodes',
            'all_vhosts': 'api/vhosts',
            'queues_by_vhost': 'api/queues/%s',
            'exchanges_by_vhost': 'api/exchanges/%s',
            'live_test': 'api/aliveness-test/%s'}

    def __init__(self, server, uname, passwd):
        self.client = httplib2.Http()
        self.client.add_credentials(uname, passwd)
        self.base_url = 'http://%s' % server

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
        vhost = namedtuple('VHost', ['name'])
        resp, content = self.do_call(os.path.join(self.base_url,
                                            HTTPClient.urls['all_vhosts']),
                                            'GET')

        vhost_data = self.decode_json_content(content)
        vhost_list = [vhost(**i) for i in vhost_data]
        return vhost_list

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

    def get_exchanges(self, vhost=None):
        if vhost:
            path = HTTPClient.urls['exchanges_by_vhost'] % vhost
        else:
            path = HTTPClient.urls['all_exchanges']

        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        exchanges = self.decode_json_content(content)
        return exchanges

    def get_connections(self):
        path = HTTPClient.urls['all_connections']
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        conns = self.decode_json_content(content)
        return conns

    def get_channels(self):
        path = HTTPClient.urls['all_channels']
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        chans = self.decode_json_content(content)
        return chans

            

class Server(object):
    """
    Abstraction of the RabbitMQ Management HTTP API.

    HTTP calls are delegated to the  HTTPClient class for ease of testing,
    cleanliness, separation of duty, flexibility, etc.
    """
    def __init__(self, host, user, passwd):
        """
        Populates server attributes using passed-in parameters and 
        the HTTP API's 'overview' information. It also instantiates
        an httplib2 HTTP client and adds credentials based on 
        passed-in initialization params. 

        """
        self.host = host
        self.client = HTTPClient(host, user, passwd)

        # populate Server instance attrs from 'overview' data.
        overview = self.client.get_overview()
        
        self.node = overview['node']
        self.mgr_version = overview['management_version']

        # returns a dict
        self.queue_totals = overview['queue_totals']

        # returns a list of dicts
        self.listeners = overview['listeners']

        self.stats_db_node = overview['statistics_db_node']
        self.message_stats = overview['message_stats']
        self.stats_level = overview['statistics_level']

        return

    def get_all_vhosts(self):
        """
        Returns a list of dicts resulting from a json.loads of the
        server response without alteration.

        """
        vhosts = self.client.get_all_vhosts()
        return vhosts

    def get_queues(self, vhost=None):
        """
        Get all queues, or all queues in a vhost if vhost is not None.
        Returns a list.

        """
        queues = self.client.get_queues(vhost)
        return queues

    def is_alive(self, vhost='%2F'):
        return self.client.is_alive(vhost)


