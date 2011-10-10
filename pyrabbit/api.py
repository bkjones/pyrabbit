from . import http
import functools
import json

class APIError(Exception):
    """Denotes a failure due to unexpected or invalid
    input/output between the client and the API

    """
    pass

class PermissionError(Exception):
    pass

def needs_admin_privs(fun):
    @functools.wraps(fun)
    def wrapper(self, *args, **kwargs):
        if self.is_admin or self.has_admin_rights:
            return fun(self, *args, **kwargs)
        else:
            raise PermissionError("Insufficient privs. User '%s'" % self.user)
    return wrapper


class Client(object):
    """
    Abstraction of the RabbitMQ Management HTTP API.

    HTTP calls are delegated to the  HTTPClient class for ease of testing,
    cleanliness, separation of duty, flexibility, etc.
    """
    urls = {'overview': 'overview',
            'all_queues': 'queues',
            'all_exchanges': 'exchanges',
            'all_channels': 'channels',
            'all_connections': 'connections',
            'all_nodes': 'nodes',
            'all_vhosts': 'vhosts',
            'all_users': 'users',
            'all_bindings': 'bindings',
            'whoami': 'whoami',
            'queues_by_vhost': 'queues/%s',
            'queues_by_name': 'queues/%s/%s',
            'exchanges_by_vhost': 'exchanges/%s',
            'exchange_by_name': 'exchanges/%s/%s',
            'live_test': 'aliveness-test/%s',
            'purge_queue': 'queues/%s/%s/contents',
            'connections_by_name': 'connections/%s',
            'bindings_by_source_exch': 'exchanges/%s/%s/bindings/source',
            'bindings_by_dest_exch': 'exchanges/%s/%s/bindings/destination',
            'bindings_on_queue': 'queues/%s/%s/bindings',
            'get_from_queue': 'queues/%s/%s/get',
            'publish_to_exchange': 'exchanges/%s/%s/publish',
            }

    def __init__(self, host, user, passwd):
        """
        :param string host: string of the form 'host:port'
        :param string user: username used to authenticate to the API.
        :param string passwd: password used to authenticate to the API.

        Populates server attributes using passed-in parameters and
        the HTTP API's 'overview' information. It also instantiates
        an httplib2 HTTP client and adds credentia    ls

        """
        self.host = host
        self.user = user
        self.passwd = passwd
        self.http = http.HTTPClient(self.host, self.user, self.passwd)

        # initialize this now. @needs_admin_privs will check this first to
        # avoid making an HTTP call. If this is None, it'll trigger an
        # HTTP call (by calling self.has_admin_rights) and populate this for
        # next time.
        self.is_admin = None
        return

    def is_alive(self, vhost='%2F'):
        """
        Uses the aliveness-test API call to determine if the
        server is alive and the vhost is active. The broker (not this code)
        creates a queue and then sends/consumes a message from it.

        :param string vhost: There should be no real reason to ever change
            this from the default value, but it's there if you need to.
        :returns bool: True if alive, False otherwise
        :raises: HTTPError if *vhost* doesn't exist on the broker.

        """
        uri = Client.urls['live_test'] % vhost

        try:
            resp = self.http.do_call(uri, 'GET')
            print resp.status
        except http.HTTPError as e:
            if e.status == 404:
                raise APIError("No vhost named '%s'" % vhost)
            raise

        if resp.status == 200:
            return True
        else:
            return False

    def get_whoami(self):
        """
        A convenience function used in the event that you need to confirm that
        the broker thinks you are who you think you are.
        """
        path = Client.urls['whoami']
        whoami  = self.http.do_call(path, 'GET')
        return whoami

    @property
    def has_admin_rights(self):
        """
        Determine if the creds passed in for authentication have admin
        rights to RabbitMQ data. If not, then there's a decent amount of
        information you can't get at.

        """
        whoami = self.get_whoami()
        if whoami.get('administrator'):
            self.is_admin = True
            return True
        else:
            return False

    def get_overview(self):
        """
        :rtype: dict

        Data in the 'overview' depends on the privileges of the creds used,
        but typically contains information about the management plugin version,
        some high-level message stats, and aggregate queue totals. Admin-level
        creds gets you information about the cluster node, listeners, etc.


        """
        overview = self.http.do_call(Client.urls['overview'], 'GET')
        return overview

    @needs_admin_privs
    def get_users(self):
        """
        :returns: a list of dictionaries, each representing a user. This
            method is decorated with '@needs_admin_privs', and will raise
            an error if the credentials used to set up the broker connection
            do not have admin privileges.

        """

        users = self.http.do_call(Client.urls['all_users'], 'GET')
        return users

    def get_all_vhosts(self):
        """
        :returns: a list of dicts, each dict representing a vhost
            on the broker.

        """
        vhosts = self.http.do_call(Client.urls['all_vhosts'], 'GET')
        return vhosts

    ##############################
    # EXCHANGES
    ##############################
    def get_exchanges(self, vhost=None):
        """
        :returns: A list of dicts
        :param string vhost: A vhost to query for exchanges, or None (default),
            which triggers a query for all exchanges in all vhosts.

        """
        if vhost:
            path = Client.urls['exchanges_by_vhost'] % vhost
        else:
            path = Client.urls['all_exchanges']

        exchanges = self.http.do_call(path, 'GET')
        return exchanges

    def get_exchange(self, vhost, name):
        """
        Gets a single exchange which requires a vhost and name.

        :param string vhost: The vhost containing the target exchange
        :param string name: The name of the exchange
        :returns: dict

        """
        path = Client.urls['exchange_by_name'] % (vhost, name)
        exch = self.http.do_call(path, 'GET')
        return exch

    def create_exchange(self,
                        vhost,
                        name,
                        type,
                        auto_delete=False,
                        durable=True,
                        internal=False,
                        arguments=None):
        """
        Creates an exchange in the given vhost with the given name. As per the
        RabbitMQ API documentation, a JSON body also needs to be included that
        "looks something like this":

        {"type":"direct",
        "auto_delete":false,
        "durable":true,
        "internal":false,
        "arguments":[]}

        On success, the API returns a 204 with no content, in which case this
        function returns True. If any other response is received, it's raised.

        :param string vhost: Vhost to create the exchange in.
        :param string name: Name of the proposed exchange.
        :param string type: The AMQP exchange type.
        :param bool auto_delete: Whether or not the exchange should be
            dropped when the no. of consumers drops to zero.
        :param bool durable: Whether you want this exchange to persist a
            broker restart.
        :param bool internal: Whether or not this is a queue for use by the
            broker only.
        :param list arguments: If given, should be a list. If not given, an
            empty list is sent.

        """

        path = Client.urls['exchange_by_name'] % (vhost, name)
        body = json.dumps({"type": type, "auto_delete": auto_delete,
                           "durable": durable, "internal": internal,
                           "arguments": arguments or []})

        self.http.do_call(path, 'PUT', body,
                          headers={'content-type': 'application/json'})
        return True

    def delete_exchange(self, vhost, name):
        """
        Delete the named exchange from the named vhost. The API returns a 204
        on success, in which case this method returns True, otherwise the
        error is raised.

        :param string vhost: Vhost where target exchange was created
        :param string name: The name of the exchange to delete.
        :returns bool: True on success.
        """
        path = Client.urls['exchange_by_name'] % (vhost, name)
        self.http.do_call(path, 'DELETE')
        return True

    #############################
    # QUEUES
    #############################
    def get_queues(self, vhost=None):
        """
        Get all queues, or all queues in a vhost if vhost is not None.
        Returns a list.

        :param string vhost: The virtual host to list queues for. If This is None
                   (the default), all queues for the broker instance are returned.
        :returns: A list of dicts, each representing a queue.
        :rtype: list of dicts

        """
        if vhost:
            path = Client.urls['all_queues'] % vhost
        else:
            path = Client.urls['all_queues']

        queues = self.http.do_call(path, 'GET')
        return queues

    def get_queue(self, vhost, name):
        """
        Get a single queue, which requires both vhost and name.

        :param string vhost: The virtual host for the queue being requested.
            If the vhost is '/', note that it will be translated to '%2F' to
            conform to URL encoding requirements.
        :param string name: The name of the queue being requested.
        :returns: A dictionary of queue properties.
        :rtype: dict

        """
        vhost = '%2F' if vhost == '/' else vhost
        path = Client.urls['queues_by_name'] % (vhost, name)
        q = self.http.do_call(path, 'GET')
        return q

    def get_queue_depth(self, vhost, name):
        """
        Get the number of messages currently in a queue. This is a convenience
         function that just calls :meth:`Client.get_queue` and pulls out/returns the 'messages'
         field from the dictionary it returns.

        :param string vhost: The vhost of the queue being queried.
        :param string name: The name of the queue to query.
        :returns: Number of messages in the queue
        :rtype: integer

        """
        vhost = '%2F' if vhost == '/' else vhost
        path = Client.urls['queues_by_name'] % (vhost, name)
        q = self.http.do_call(path, 'GET')
        depth = q['messages']
        return depth

    def purge_queues(self, queues):
        """
        Purge all messages from one or more queues.

        :param list queues: A list of ('qname', 'vhost') tuples.
        :returns: True on success

        """
        for name, vhost in queues:
            vhost = '%2F' if vhost =='/' else vhost
            path = Client.urls['purge_queue'] % (vhost, name)
            self.http.do_call(path, 'DELETE')
        return True

    def purge_queue(self, vhost, name):
        """
        Purge all messages from a single queue. This is a convenience method
        so you aren't forced to supply a list containing a single tuple to
        the purge_queues method.

        :param string vhost: The vhost of the queue being purged.
        :param string name: The name of the queue being purged.
        :rtype: None

        """
        vhost = '%2F' if vhost == '/' else vhost
        path = Client.urls['purge_queue'] % (vhost, name)
        self.http.do_call(path, 'DELETE')
        return

    def create_queue(self,name, vhost, auto_delete=False, durable=True,
                         arguments=None, node='rabbit@localhost'):
        """
        Create a queue. This is just a passthrough to the http client method
        of the same name. The args are identical (see :mod:`pyrabbit.http`)
        """
        path = Client.urls['queues_by_name'] % (vhost, name)
        body = json.dumps({"auto_delete": auto_delete,  "durable": durable,
                                   "arguments": arguments or [], "node": node})
        return self.http.do_call(path,
                                 'PUT',
                                 body,
                                 headers={'content-type': 'application/json'})

    #########################################
    # CONNS/CHANS & BINDINGS
    #########################################
    def get_connections(self):
        """
        :returns: list of dicts, or an empty list if there are no connections.
        """
        path = Client.urls['all_connections']
        conns = self.http.do_call(path, 'GET')
        return conns

    def get_connection(self, name):
        path = Client.urls['connections_by_name'] % name
        conn = self.http.do_call(path, 'GET')
        return conn

    def get_channels(self):
        """
        Return a list of dicts containing details about broker connections.
        :returns: list of dicts
        """
        path = Client.urls['all_channels']
        chans = self.http.do_call(path, 'GET')
        return chans

    def get_bindings(self):
        """
        Returns a list of dicts.

        """
        path = Client.urls['all_bindings']
        bindings = self.http.do_call(path, 'GET')
        return bindings
