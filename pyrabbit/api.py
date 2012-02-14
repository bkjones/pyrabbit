"""
The api module houses the Client class, which provides the main interface
developers will use to interact with RabbitMQ. It also contains errors and
decorators used by the class.
"""

from . import http
import functools
import json


class APIError(Exception):
    """Denotes a failure due to unexpected or invalid
    input/output between the client and the API

    """
    pass


class PermissionError(Exception):
    """
    Raised if the operation requires admin permissions, and the user used to
    instantiate the Client class does not have admin privileges.
    """
    pass


def needs_admin_privs(fun):
    """
    A decorator that can be added to any of the Client methods in order to
    indicate that admin privileges should be checked for before issuing an
    HTTP call (if possible - if Client.is_admin isn't set, an HTTP call is
    made to find out).

    """
    @functools.wraps(fun)
    def wrapper(self, *args, **kwargs):
        """
        This is the function that runs in place of the one being decorated.

        """
        if self.has_admin_rights:
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
            'bindings_between_exch_queue': 'bindings/%s/e/%s/q/%s',
            'get_from_queue': 'queues/%s/%s/get',
            'publish_to_exchange': 'exchanges/%s/%s/publish',
            'vhosts_by_name': 'vhosts/%s',
            'vhost_permissions': 'permissions/%s/%s'
            }

    json_headers = {"content-type": "application/json"}

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

    @needs_admin_privs
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
        except http.HTTPError as err:
            if err.status == 404:
                raise APIError("No vhost named '%s'" % vhost)
            raise

        if resp['status'] == 'ok':
            return True
        else:
            return False

    def get_whoami(self):
        """
        A convenience function used in the event that you need to confirm that
        the broker thinks you are who you think you are.

        :returns dict whoami: Dict structure contains:
            * administrator: whether the user is has admin privileges
            * name: user name
            * auth_backend: backend used to determine admin rights
        """
        path = Client.urls['whoami']
        whoami = self.http.do_call(path, 'GET')
        return whoami

    @property
    def has_admin_rights(self):
        """
        Determine if the creds passed in for authentication have admin
        rights to RabbitMQ data. If not, then there's a decent amount of
        information you can't get at.

        :returns bool is_admin: True if self.user has admin rights.

        """
        if self.is_admin is None:
            whoami = self.get_whoami()
            self.is_admin = whoami.get('administrator', False)

        return self.is_admin

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

    ################################################
    ###         VHOSTS
    ################################################
    def get_all_vhosts(self):
        """
        :returns: a list of dicts, each dict representing a vhost
            on the broker.

        """
        vhosts = self.http.do_call(Client.urls['all_vhosts'], 'GET')
        return vhosts

    def get_vhost_names(self):
        """
        A convenience function for getting back only the vhost names instead of
        the larger vhost dicts.

        :returns list vhost_names: A list of just the vhost names.
        """
        vhosts = self.get_all_vhosts()
        vhost_names = [i['name'] for i in vhosts]
        return vhost_names

    def get_vhost(self, vname):
        """
        Returns the attributes of a single named vhost in a dict.

        :param string vname: Name of the vhost to get.
        :returns dict vhost: Attribute dict for the named vhost

        """

        path = Client.urls['vhosts_by_name'] % vname
        vhost = self.http.do_call(path, 'GET', headers=Client.json_headers)
        return vhost

    def create_vhost(self, vname):
        """
        Creates a vhost on the server to house exchanges.

        :param string vname: The name to give to the vhost on the server
        :returns: boolean
        """
        path = Client.urls['vhosts_by_name'] % vname
        return self.http.do_call(path, 'PUT',
                                 headers=Client.json_headers)

    def delete_vhost(self, vname):
        """
        Deletes a vhost from the server. Note that this also deletes any
        exchanges or queues that belong to this vhost.

        :param string vname: Name of the vhost to delete from the server.
        """
        path = Client.urls['vhosts_by_name'] % vname
        return self.http.do_call(path, 'DELETE')

    def set_vhost_permissions(self, vname, username, config, rd, wr):
        """
        Set permissions for a given username on a given vhost. Both
        must already exist.

        :param string vname: Name of the vhost to set perms on.
        :param string username: User to set permissions for.
        :param string config: Permission pattern for configuration operations
            for this user in this vhost.
        :param string rd: Permission pattern for read operations for this user
            in this vhost
        :param string wr: Permission pattern for write operations for this user
            in this vhost.

        Permission patterns are regex strings. If you're unfamiliar with this,
        you should definitely check out this section of the RabbitMQ docs:

        http://www.rabbitmq.com/admin-guide.html#access-control
        """
        body = json.dumps({"configure": config, "read": rd, "write": wr})
        path = Client.urls['vhost_permissions'] % (vname, username)
        return self.http.do_call(path, 'PUT', body,
                                 headers=Client.json_headers)

    ###############################################
    ##           EXCHANGES
    ###############################################
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
                        xtype,
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
        base_body = {"type": xtype, "auto_delete": auto_delete,
                           "durable": durable, "internal": internal}
        if arguments:
            base_body['arguments'] = arguments

        body = json.dumps(base_body)
        self.http.do_call(path, 'PUT', body,
                          headers=Client.json_headers)
        return True

    def publish(self, vhost, xname, rt_key, payload, payload_enc='string',
                properties=None):
        """
        Publish a message to an exchange.

        :param string vhost: vhost housing the target exchange
        :param string xname: name of the target exchange
        :param string rt_key: routing key for message
        :param string payload: the message body for publishing
        :param string payload_enc: encoding of the payload. The only choices
                      here are 'string' and 'base64'.
        :param dict properties: a dict of message properties
        :returns: boolean indicating success or failure.
        """
        path = Client.urls['publish_to_exchange'] % (vhost, xname)
        body = json.dumps({'routing_key': rt_key, 'payload': payload,
                           'payload_encoding': payload_enc,
                           'properties': properties or {}})
        result = self.http.do_call(path, 'POST', body)
        return result['routed']

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

    #############################################
    ##              QUEUES
    #############################################
    def get_queues(self, vhost=None):
        """
        Get all queues, or all queues in a vhost if vhost is not None.
        Returns a list.

        :param string vhost: The virtual host to list queues for. If This is
                    None (the default), all queues for the broker instance
                    are returned.
        :returns: A list of dicts, each representing a queue.
        :rtype: list of dicts

        """
        if vhost:
            path = Client.urls['queues_by_vhost'] % vhost
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
        queue = self.http.do_call(path, 'GET')
        return queue

    def get_queue_depth(self, vhost, name):
        """
        Get the number of messages currently in a queue. This is a convenience
         function that just calls :meth:`Client.get_queue` and pulls
         out/returns the 'messages' field from the dictionary it returns.

        :param string vhost: The vhost of the queue being queried.
        :param string name: The name of the queue to query.
        :returns: Number of messages in the queue
        :rtype: integer

        """
        vhost = '%2F' if vhost == '/' else vhost
        path = Client.urls['queues_by_name'] % (vhost, name)
        queue = self.http.do_call(path,'GET')
        depth = 0

        if isinstance(queue, list):
            for q in queue:
                if q['name'] == name:
                    depth = q['messages']
        else:
            depth = queue['messages']

        return depth

    def purge_queues(self, queues):
        """
        Purge all messages from one or more queues.

        :param list queues: A list of ('qname', 'vhost') tuples.
        :returns: True on success

        """
        for name, vhost in queues:
            vhost = '%2F' if vhost == '/' else vhost
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
        return self.http.do_call(path, 'DELETE')

    def create_queue(self, name, vhost, auto_delete=None, durable=None,
                         arguments=None, node=None):
        """
        Create a queue. The API documentation specifies that all of the body
        elements are optional, so this method only requires arguments needed
        to form the URI

        :param string name: The name of the queue
        :param string vhost: The vhost to create the queue in.
        :param string auto_delete: Whether to destroy the queue when the
            consumer count is zero (after having been non-zero at some point)
        :param string durable: Whether the queue should exist after a restart
        :param string arguments: Arguments for the queue declaration.
        :param string node: The erlang/rabbit node name to create the queue on.

        More on these operations can be found at:
        http://www.rabbitmq.com/amqp-0-9-1-reference.html

        """

        if auto_delete or durable or arguments or node:
            base_body = {"auto_delete": auto_delete,
                       "durable": durable,
                       "node": node}
            if arguments:
                base_body['arguments'] = arguments
        else:
            base_body = {}

        body = json.dumps(base_body)

        path = Client.urls['queues_by_name'] % (vhost, name)
        return self.http.do_call(path,
                                 'PUT',
                                 body,
                                 headers=Client.json_headers)

    def delete_queue(self, vhost, qname):
        """
        Deletes the named queue from the named vhost.

        :param string vhost: Vhost housing the queue to be deleted.
        :param string qname: Name of the queue to delete.

        Note that if you just want to delete the messages from a queue, you
        should use purge_queue instead of deleting/recreating a queue.
        """
        path = Client.urls['queues_by_name'] % (vhost, qname)
        return self.http.do_call(path, 'DELETE', headers=Client.json_headers)

    def get_messages(self, vhost, qname, count=1,
                     requeue=False, truncate=None, encoding='auto'):
        """
        Gets <count> messages from the queue.

        :param string vhost: Name of vhost containing the queue
        :param string qname: Name of the queue to consume from
        :param int count: Number of messages to get.
        :param bool requeue: Whether to requeue the message after getting it.
            This will cause the 'redelivered' flag to be set in the message on
            the queue.
        :param int truncate: The length, in bytes, beyond which the server will
            truncate the message before returning it.
        :returns: list of dicts. messages[msg-index]['payload'] will contain
                the message body.
        """

        base_body = {'count': count, 'requeue': requeue, 'encoding': encoding}
        if truncate:
            base_body['truncate'] = truncate
        body = json.dumps(base_body)

        path = Client.urls['get_from_queue'] % (vhost, qname)
        messages = self.http.do_call(path, 'POST', body,
                                     headers=Client.json_headers)
        return messages

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
        """
        Get a connection by name. To get the names, use get_connections.

        :param string name: Name of connection to get
        :returns dict conn: A connection attribute dictionary.

        """
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
        :returns: list of dicts

        """
        path = Client.urls['all_bindings']
        bindings = self.http.do_call(path, 'GET')
        return bindings

    def create_binding(self, vhost, exchange, queue, rt_key=None, args=None):
        """
        Creates a binding between an exchange and a queue on a given vhost.

        :param string vhost: vhost housing the exchange/queue to bind
        :param string exchange: the target exchange of the binding
        :param string queue: the queue to bind to the exchange
        :param string rt_key: the routing key to use for the binding
        :param list args: extra arguments to associate w/ the binding.
        :returns: boolean
        """

        body = json.dumps({'routing_key': rt_key, 'arguments': args or []})
        path = Client.urls['bindings_between_exch_queue'] % (vhost,
                                                             exchange,
                                                             queue)
        binding = self.http.do_call(path, 'POST', body=body,
                                    headers=Client.json_headers)
        return binding
