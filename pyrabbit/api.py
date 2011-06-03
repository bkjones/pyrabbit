from collections import namedtuple
from . import http

vhost = namedtuple('VHost', ['name'])

# initialize a vhost 'object' w/ defaults.
prototype_vhost = vhost(*[None for i in vhost._fields])

exch = namedtuple('Exchange', ['name', 'vhost', 'type',
                               'durable', 'auto_delete', 'internal',
                               'arguments'])

# initialize an exch 'object' w/ defaults.
prototype_exch = exch(*[None for i in exch._fields])

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

# initialize a conn 'object' w/ defaults.
prototype_conn = conn(*[None for i in conn._fields])

queue = namedtuple('Queue', ['memory', 'messages', 'consumer_details',
                             'idle_since', 'exclusive_consumer_pid',
                             'exclusive_consumer_tag', 'messages_ready',
                             'messages_unacknowledged', 'consumers',
                             'backing_queue_status', 'name',
                             'vhost', 'durable', 'auto_delete',
                             'owner_pid', 'arguments', 'pid', 'node'])

# initialize a queue 'object' w/ defaults.
prototype_queue = queue(*[None for i in queue._fields])

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
        self.client = http.HTTPClient(host, user, passwd)

        overview = self.client.get_overview()

        #TODO blech. This is basically an object whose members are dynamic, so
        # self.overview.foo could work in some places and not others, depending
        # on perms, configuration, activity (or lack thereof), etc.
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
        vhost_list = [prototype_vhost._replace(**i) for i in vhosts]
        return vhost_list

    def get_queues(self, vhost=None):
        """
        Get all queues, or all queues in a vhost if vhost is not None.
        Returns a list.

        """
        queues = self.client.get_queues(vhost)
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


