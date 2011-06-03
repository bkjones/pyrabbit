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

vhost = namedtuple('VHost', ['name'])

exch = namedtuple('Exchange', ['name', 'vhost', 'type',
                               'durable', 'auto_delete', 'internal',
                               'arguments'])

conn = namedtuple("Connection", ['frame_max', 'send_pend',
                                 'peer_cert_validity',
                                 'client_properties', 'ssl_protocol',
                                 'pid', 'channels', 'auth_mechanism',
                                 'peer_cert_issuer',
                                 'peer_cert_subject', 'peer_address',
                                 'port', 'send_oct_details',
                                 'recv_cnt', 'send_oct', 'protocol',
                                 'recv_oct_details', 'state',
                                 'ssl_cipher', 'node', 'timeout',
                                 'peer_port', 'ssl', 'vhost', 'user',
                                 'address', 'name', 'ssl_hash',
                                 'recv_oct', 'send_cnt',
                                 'ssl_key_exchange'])

queue = namedtuple('Queue', ['memory', 'messages', 'consumer_details',
                             'idle_since', 'exclusive_consumer_pid',
                             'exclusive_consumer_tag', 'messages_ready',
                             'messages_unacknowledged', 'consumers',
                             'backing_queue_status', 'name',
                             'vhost', 'durable', 'auto_delete',
                             'owner_pid', 'arguments', 'pid', 'node'])

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
        an httplib2 HTTP client and adds credentials

        """
        self.host = host
        self.client = HTTPClient(host, user, passwd)

        overview = self.client.get_overview()
        self.overview = namedtuple('Overview', list(overview.keys()))(**overview)
        
        return

    def get_all_vhosts(self):
        """
        Returns a list of namedtuples that act and are in all ways
        equivalent to instances of type VHost. Since all operations on
        these objects are done via the API provided by this Server class,
        VHosts are just bags of vhost attributes that are addressable using
        cleaner, more meaningful syntax than objects
        (e.g. 'v.name' instead of 'v[0]')

        """
        vhosts = self.client.get_all_vhosts()
        vhost_list = [vhost(**i) for i in vhosts]
        return vhost_list

    def get_queues(self, vhost=None):
        """
        Get all queues, or all queues in a vhost if vhost is not None.
        Returns a list.

        """
        queues = self.client.get_queues(vhost)

        # initialize a queue 'object' w/ defaults.
        prototype_queue = queue(*[None for i in queue._fields])
        queue_list = [prototype_queue._replace(**i) for i in queues]
        return queue_list

    def get_queue(self, vhost, name):
        """
        Get a single queue, which requires both vhost and name.

        """
        q = self.client.get_queue(vhost, name)
        queue_out = queue(**q)
        return queue_out

    def purge_queues(self, queues):
        """
        The queues parameter should be a list of one or more queue objects.

        """
        for q in queues:
            vhost = q.vhost
            vhost = '%2F' if vhost =='/' else vhost
            name = q.name
            self.client.purge_queue(vhost, name)
        return True

    def get_connections(self, name=None):
        """
        Returns a list of one or more Connection namedtuple objects

        """
        connections = self.client.get_connections(name)
        connlist = [conn(**i) for i in connections]
        return connlist

    def get_exchanges(self, vhost=None):
        """
        Returns a list of Exchange namedtuple objects.

        """
        xchs = self.client.get_exchanges(vhost)
        xlist = [exch(**i) for i in xchs]
        return xlist

    def get_exchange(self, vhost, xname):
        """
        Returns a single exchange namedtuple subclass.

        """
        xch = self.client.get_exchange(vhost, xname)
        out_exch = exch(**xch)
        return out_exch

    def is_alive(self, vhost='%2F'):
        return self.client.is_alive(vhost)


