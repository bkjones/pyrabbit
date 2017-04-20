"""
The api module houses the Client class, which provides the main interface
developers will use to interact with RabbitMQ. It also contains errors and
decorators used by the class.
"""

from . import http
import functools
import json
try:
    # python 2.x
    from urllib import quote
except ImportError:
    # python 3.x
    from urllib.parse import quote


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
            'all_permissions': 'permissions',
            'all_bindings': 'bindings',
            'whoami': 'whoami',
            'queues_by_vhost': 'queues/%s',
            'queues_by_name': 'queues/%s/%s',
            'exchanges_by_vhost': 'exchanges/%s',
            'exchange_by_name': 'exchanges/%s/%s',
            'live_test': 'aliveness-test/%s',
            'purge_queue': 'queues/%s/%s/contents',
            'channels_by_name': 'channels/%s',
            'connections_by_name': 'connections/%s',
            'bindings_by_source_exch': 'exchanges/%s/%s/bindings/source',
            'bindings_by_dest_exch': 'exchanges/%s/%s/bindings/destination',
            'bindings_on_queue': 'queues/%s/%s/bindings',
            'bindings_between_exch_queue': 'bindings/%s/e/%s/q/%s',
            'rt_bindings_between_exch_queue': 'bindings/%s/e/%s/q/%s/%s',
            'get_from_queue': 'queues/%s/%s/get',
            'publish_to_exchange': 'exchanges/%s/%s/publish',
            'vhosts_by_name': 'vhosts/%s',
            'vhost_permissions': 'permissions/%s/%s',
            'users_by_name': 'users/%s',
            'user_permissions': 'users/%s/permissions',
            'vhost_permissions_get': 'vhosts/%s/permissions',
            'definitions': 'definitions'}

    json_headers = {"content-type": "application/json"}

    def __init__(self, api_url, user, passwd, timeout=5, scheme='http'):
        """
        :param string api_url: base url for the broker API
        :param string user: Username used to authenticate to the API.
        :param string passwd: Password used to authenticate to the API.
        :param int timeout: Integer number of seconds to wait for each call.
        :param string scheme: HTTP scheme used to make the connection

        Populates server attributes using passed-in parameters and
        the HTTP API's 'overview' information.
        """
        self.api_url = api_url
        self.user = user
        self.passwd = passwd
        self.timeout = timeout
        self.scheme = scheme
        self.http = http.HTTPClient(
            self.api_url,
            self.user,
            self.passwd,
            self.timeout,
            self.scheme
        )

        return

    def _call(self, path, method, body=None, headers=None):
        """
        Wrapper around http.do_call that transforms some HTTPError into
        our own exceptions
        """
        try:
            resp = self.http.do_call(path, method, body, headers)
        except http.HTTPError as err:
            if err.status == 401:
                raise PermissionError('Insufficient permissions to query ' +
                    '%s with user %s :%s' % (path, self.user, err))
            raise
        return resp


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
            resp = self._call(uri, 'GET')
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
        whoami = self._call(path, 'GET')
        return whoami

    def get_overview(self):
        """
        :rtype: dict

        Data in the 'overview' depends on the privileges of the creds used,
        but typically contains information about the management plugin version,
        some high-level message stats, and aggregate queue totals. Admin-level
        creds gets you information about the cluster node, listeners, etc.

        """
        overview = self._call(Client.urls['overview'], 'GET')
        return overview

    def get_nodes(self):
        """
        :rtype: dict

        Returns a list of dictionaries, each containing the details of each
        node of the cluster.


        """
        nodes = self._call(Client.urls['all_nodes'], 'GET')
        return nodes

    def get_users(self):
        """
        Returns a list of dictionaries, each containing the attributes of a
        different RabbitMQ user.

        :returns: a list of dictionaries, each representing a user.

        """
        users = self._call(Client.urls['all_users'], 'GET')
        return users

    ################################################
    ###         VHOSTS
    ################################################
    def get_all_vhosts(self):
        """
        Lists the names of all RabbitMQ vhosts.

        :returns: a list of dicts, each dict representing a vhost
                on the broker.

        """
        vhosts = self._call(Client.urls['all_vhosts'], 'GET')
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

        vname = quote(vname, '')
        path = Client.urls['vhosts_by_name'] % vname
        vhost = self._call(path, 'GET', headers=Client.json_headers)
        return vhost

    def create_vhost(self, vname):
        """
        Creates a vhost on the server to house exchanges.

        :param string vname: The name to give to the vhost on the server
        :returns: boolean
        """
        vname = quote(vname, '')
        path = Client.urls['vhosts_by_name'] % vname
        return self._call(path, 'PUT',
                                 headers=Client.json_headers)

    def delete_vhost(self, vname):
        """
        Deletes a vhost from the server. Note that this also deletes any
        exchanges or queues that belong to this vhost.

        :param string vname: Name of the vhost to delete from the server.
        """
        vname = quote(vname, '')
        path = Client.urls['vhosts_by_name'] % vname
        return self._call(path, 'DELETE')

    ###############################################
    ##           PERMISSIONS
    ###############################################
    def get_permissions(self):
        """
        :returns: list of dicts, or an empty list if there are no permissions.
        """
        path = Client.urls['all_permissions']
        conns = self._call(path, 'GET')
        return conns

    def get_vhost_permissions(self, vname):
        """
        :returns: list of dicts, or an empty list if there are no permissions.

        :param string vname: Name of the vhost to set perms on.
        """
        vname = quote(vname, '')
        path = Client.urls['vhost_permissions_get'] % (vname,)
        conns = self._call(path, 'GET')
        return conns

    def get_user_permissions(self, username):
        """
        :returns: list of dicts, or an empty list if there are no permissions.

        :param string username: User to set permissions for.
        """

        path = Client.urls['user_permissions'] % (username,)
        conns = self._call(path, 'GET')
        return conns

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
        vname = quote(vname, '')
        body = json.dumps({"configure": config, "read": rd, "write": wr})
        path = Client.urls['vhost_permissions'] % (vname, username)
        return self._call(path, 'PUT', body,
                                 headers=Client.json_headers)

    def delete_permission(self, vname, username):
        """
        Delete permission for a given username on a given vhost. Both
        must already exist.

        :param string vname: Name of the vhost to set perms on.
        :param string username: User to set permissions for.
        """
        vname = quote(vname, '')
        path = Client.urls['vhost_permissions'] % (vname, username)
        return self._call(path, 'DELETE')

    def get_permission(self, vname, username):
        """
        :returns: dicts of permissions.

        :param string vname: Name of the vhost to set perms on.
        :param string username: User to set permissions for.
        """
        vname = quote(vname, '')
        path = Client.urls['vhost_permissions'] % (vname, username)
        return self._call(path, 'GET')

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
            vhost = quote(vhost, '')
            path = Client.urls['exchanges_by_vhost'] % vhost
        else:
            path = Client.urls['all_exchanges']

        exchanges = self._call(path, 'GET')
        return exchanges

    def get_exchange(self, vhost, name):
        """
        Gets a single exchange which requires a vhost and name.

        :param string vhost: The vhost containing the target exchange
        :param string name: The name of the exchange
        :returns: dict

        """
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['exchange_by_name'] % (vhost, name)
        exch = self._call(path, 'GET')
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

        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['exchange_by_name'] % (vhost, name)
        base_body = {"type": xtype, "auto_delete": auto_delete,
                     "durable": durable, "internal": internal,
                     "arguments": arguments or list()}

        body = json.dumps(base_body)
        self._call(path, 'PUT', body,
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
        vhost = quote(vhost, '')
        xname = quote(xname, '')
        path = Client.urls['publish_to_exchange'] % (vhost, xname)
        body = json.dumps({'routing_key': rt_key, 'payload': payload,
                           'payload_encoding': payload_enc,
                           'properties': properties or {}})
        result = self._call(path, 'POST', body)
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
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['exchange_by_name'] % (vhost, name)
        self._call(path, 'DELETE')
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
            vhost = quote(vhost, '')
            path = Client.urls['queues_by_vhost'] % vhost
        else:
            path = Client.urls['all_queues']

        queues = self._call(path, 'GET')
        return queues or list()

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
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['queues_by_name'] % (vhost, name)
        queue = self._call(path, 'GET')
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
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['queues_by_name'] % (vhost, name)
        queue = self._call(path, 'GET')
        depth = queue['messages']

        return depth

    def get_queue_depths(self, vhost, names=None):
        """
        Get the number of messages currently sitting in either the queue
        names listed in 'names', or all queues in 'vhost' if no 'names' are
        given.

        :param str vhost: Vhost where queues in 'names' live.
        :param list names: OPTIONAL - Specific queues to show depths for. If
                None, show depths for all queues in 'vhost'.
        """

        vhost = quote(vhost, '')
        if not names:
            # get all queues in vhost
            path = Client.urls['queues_by_vhost'] % vhost
            queues = self._call(path, 'GET')
            for queue in queues:
                depth = queue['messages']
                print("\t%s: %s" % (queue, depth))
        else:
            # get the named queues only.
            for name in names:
                depth = self.get_queue_depth(vhost, name)
                print("\t%s: %s" % (name, depth))

    def purge_queues(self, queues):
        """
        Purge all messages from one or more queues.

        :param list queues: A list of ('qname', 'vhost') tuples.
        :returns: True on success

        """
        for name, vhost in queues:
            vhost = quote(vhost, '')
            name = quote(name, '')
            path = Client.urls['purge_queue'] % (vhost, name)
            self._call(path, 'DELETE')
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
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['purge_queue'] % (vhost, name)
        return self._call(path, 'DELETE')

    def create_queue(self, vhost, name, **kwargs):
        """
        Create a queue. The API documentation specifies that all of the body
        elements are optional, so this method only requires arguments needed
        to form the URI

        :param string vhost: The vhost to create the queue in.
        :param string name: The name of the queue

        More on these operations can be found at:
        http://www.rabbitmq.com/amqp-0-9-1-reference.html

        """

        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['queues_by_name'] % (vhost, name)

        body = json.dumps(kwargs)

        return self._call(path,
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
        vhost = quote(vhost, '')
        qname = quote(qname, '')
        path = Client.urls['queues_by_name'] % (vhost, qname)
        return self._call(path, 'DELETE', headers=Client.json_headers)

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

        vhost = quote(vhost, '')
        base_body = {'count': count, 'requeue': requeue, 'encoding': encoding}
        if truncate:
            base_body['truncate'] = truncate
        body = json.dumps(base_body)

        qname = quote(qname, '')
        path = Client.urls['get_from_queue'] % (vhost, qname)
        messages = self._call(path, 'POST', body,
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
        conns = self._call(path, 'GET')
        return conns

    def get_connection(self, name):
        """
        Get a connection by name. To get the names, use get_connections.

        :param string name: Name of connection to get
        :returns dict conn: A connection attribute dictionary.

        """
        name = quote(name, '')
        path = Client.urls['connections_by_name'] % name
        conn = self._call(path, 'GET')
        return conn

    def delete_connection(self, name):
        """
        Close the named connection. The API returns a 204 on success,
        in which case this method returns True, otherwise the
        error is raised.

        :param string name: The name of the connection to delete.
        :returns bool: True on success.
        """
        name = quote(name, '')
        path = Client.urls['connections_by_name'] % name
        self._call(path, 'DELETE')
        return True

    def get_channels(self):
        """
        Return a list of dicts containing details about broker connections.
        :returns: list of dicts
        """
        path = Client.urls['all_channels']
        chans = self._call(path, 'GET')
        return chans

    def get_channel(self, name):
        """
        Get a channel by name. To get the names, use get_channels.

        :param string name: Name of channel to get
        :returns dict conn: A channel attribute dictionary.

        """
        name = quote(name, '')
        path = Client.urls['channels_by_name'] % name
        chan = self._call(path, 'GET')
        return chan

    def get_bindings(self):
        """
        :returns: list of dicts

        """
        path = Client.urls['all_bindings']
        bindings = self._call(path, 'GET')
        return bindings

    def get_queue_bindings(self, vhost, qname):
        """
        Return a list of dicts, one dict per binding. The dict format coming
        from RabbitMQ for queue named 'testq' is:

        {"source":"sourceExch","vhost":"/","destination":"testq",
         "destination_type":"queue","routing_key":"*.*","arguments":{},
         "properties_key":"%2A.%2A"}
        """
        vhost = quote(vhost, '')
        qname = quote(qname, '')
        path = Client.urls['bindings_on_queue'] % (vhost, qname)
        bindings = self._call(path, 'GET')
        return bindings

    def get_bindings_from_exchange(self, vhost, exch):
        pass

    def get_bindings_to_exchange(self, vhost, exch):
        pass

    def get_bindings_between_exch_and_queue(self, vhost, exch, queue):
        pass

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

        vhost = quote(vhost, '')
        exchange = quote(exchange, '')
        queue = quote(queue, '')
        body = json.dumps({'routing_key': rt_key, 'arguments': args or []})
        path = Client.urls['bindings_between_exch_queue'] % (vhost,
                                                             exchange,
                                                             queue)
        binding = self._call(path, 'POST', body=body,
                                    headers=Client.json_headers)
        return binding

    def delete_binding(self, vhost, exchange, queue, rt_key):
        """
        Deletes a binding between an exchange and a queue on a given vhost.

        :param string vhost: vhost housing the exchange/queue to bind
        :param string exchange: the target exchange of the binding
        :param string queue: the queue to bind to the exchange
        :param string rt_key: the routing key to use for the binding
        """

        vhost = quote(vhost, '')
        exchange = quote(exchange, '')
        queue = quote(queue, '')
        body = ''
        path = Client.urls['rt_bindings_between_exch_queue'] % (vhost,
                                                                exchange,
                                                                queue,
                                                                rt_key)
        return self._call(path, 'DELETE', headers=Client.json_headers)

    def create_user(self, username, password, tags=""):
        """
        Creates a user.

        :param string username: The name to give to the new user
        :param string password: Password for the new user
        :param string tags: Comma-separated list of tags for the user
        :returns: boolean
        """
        path = Client.urls['users_by_name'] % username
        body = json.dumps({'password': password, 'tags': tags})
        return self._call(path, 'PUT', body=body,
                                 headers=Client.json_headers)

    def delete_user(self, username):
        """
        Deletes a user from the server.

        :param string username: Name of the user to delete from the server.
        """
        path = Client.urls['users_by_name'] % username
        return self._call(path, 'DELETE')


    def get_definitions(self):
        """
        Get definitions
        """
        path = Client.urls['definitions']
        definitions = self.http.do_call(path, 'GET')
        return definitions


    def set_definitions(self, definitions):
        """
        Set definitions
        """
        body = json.dumps(definitions)
        path = Client.urls['definitions']
        definitions = self.http.do_call(path, 'POST', body,
                                     headers=Client.json_headers)
        return definitions
