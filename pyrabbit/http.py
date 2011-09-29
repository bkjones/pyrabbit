
import httplib2
import json
import os

class HTTPError(Exception):
    """
    An error response from the API server. This should be an
    HTTP error of some kind (404, 500, etc).

    """
    def __init__(self, status=None, reason=None, path=None):
        self.status = status
        self.reason = reason
        self.path = path
        self.output = "%s - %s (%s)" % (self.status, self.reason, self.path)

    def __str__(self):
        return self.output

class NetworkError(Exception):
    """Denotes a failure to communicate with the REST API

    """
    pass

class APIError(Exception):
    """Denotes a failure due to unexpected or invalid 
    input/output between the client and the API

    """
    pass

class HTTPClient(object):
    """
    A wrapper for (currently) httplib2. Abstracts away 
    things like path building, return value parsing, etc., 
    so the api module code stays clean and easy to read/use.

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
        py_ct = json.loads(str_ct)
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
        try:
            resp, content = self.client.request(path,
                                                reqtype,
                                                body or '',
                                                headers or {})
        except Exception as out:
            # net-related exception types from httplib2 are unpredictable.
            raise NetworkError("Error: %s %s" % (type(out), out))

        if resp.status != 200 and resp.status != 204:
            raise HTTPError(resp.status, resp.reason, path)
        else:
            return resp, content

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
        """
        A convenience function used in the event that you need to confirm that
        the broker thinks you are who you think you are.
        """
        path = os.path.join(self.base_url, HTTPClient.urls['whoami'])
        resp, content = self.do_call(path, 'GET')
        whoami = self.decode_json_content(content)
        return whoami

    def get_users(self):
        """
        Returns a list of all users. Requires admin privileges. This method
        raises APIError when the broker returns a 401. This is mostly for cases
        where this class is used directly. The intent is actually to have all
        application code utilize :mod:`pyrabbit.api` to interact with the REST
        interface, and that module applies the api.needs_admin_privs decorator
        to methods of that module requiring admin rights. Therefore, the
        api module code should actually never have to handle the APIError
        raised here.
        """
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
        Provides a dictionary of miscellaneous data about the
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

        :param string vhost: Vhost to query for queues. By default this is
            None, which triggers a request for all queues in all vhosts.

        """
        if vhost:
            path = HTTPClient.urls['queues_by_vhost'] % vhost
        else:
            path = HTTPClient.urls['all_queues']

        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        queues = self.decode_json_content(content)
        return queues

    def get_queue(self, vhost, name):
        """
        Get information about a specific queue.

        :param string vhost: The vhost containing the target queue.
        :param string name: The name of the queue.

        """
        path = HTTPClient.urls['queues_by_name'] % (vhost, name)
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        queue = self.decode_json_content(content)
        return queue


    def purge_queue(self, vhost, name):
        """
        Clears a specific queue of all messages.

        :param string vhost: The vhost containing the queue to purge.

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

    def create_queue(self, name, vhost, auto_delete=False, durable=True,
                     arguments=None, node='rabbit@localhost'):
        """
        Creates a queue of the given name in the given vhost. Per API docs,
        the body of the message should look 'something like this':

        {"auto_delete":false,"durable":true,"arguments":[],"node":"rabbit@smacmullen"}

        :param string name: A name for the queue
        :param string vhost: The existing vhost to put the new queue into.
        :param bool auto_delete: Whether the queue should go away when there
                are no consumers
        :param bool durable: Whether the queue should persist a reboot.
        :param list arguments: I have no idea what this is. TODO: find out.
        :param string node: The rabbit node name to create the queue on.
        """
        path = HTTPClient.urls['queues_by_name'] % (vhost, name)
        body = json.dumps({"auto_delete": auto_delete,  "durable": durable,
                           "arguments": arguments or [], "node": node})
        self.do_call(os.path.join(self.base_url, path),
                     'PUT',
                     body,
                     headers={'content-type': 'application/json'})
        return True

    def get_exchanges(self, vhost=None):
        """
        :returns: A list of dicts
        :param string vhost: A vhost to query for exchanges, or None (default),
            which triggers a query for all exchanges in all vhosts.

        """
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

        :param string vhost: The vhost containing the target exchange
        :param string name: The name of the exchange
        :returns: dict

        """
        path = HTTPClient.urls['exchange_by_name'] % (vhost, name)
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        exch = self.decode_json_content(content)
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

        path = HTTPClient.urls['exchange_by_name'] % (vhost, name)
        body = json.dumps({"type": type, "auto_delete": auto_delete,
                           "durable": durable, "internal": internal,
                           "arguments": arguments or []})
        self.do_call(os.path.join(self.base_url, path),
                                  'PUT',
                                  body,
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
        path = HTTPClient.urls['exchange_by_name'] % (vhost, name)
        self.do_call(os.path.join(self.base_url, path), 'DELETE')
        return True

    def get_connections(self):
        """
        :returns: list of dicts, or an empty list if there are no connections.
        :param string name: The name of a specific connection to get
        """
        path = HTTPClient.urls['all_connections']
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        conns = self.decode_json_content(content)
        return conns

    def get_connection(self, name):
        path = HTTPClient.urls['connections_by_name']
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        conn = self.decode_json_content(content)
        return conn

    def get_channels(self):
        """
        Return a list of dicts containing details about broker connections.
        :returns: list of dicts
        """
        path = HTTPClient.urls['all_channels']
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        chans = self.decode_json_content(content)
        return chans
    
    def get_bindings(self):
        """
        Returns a list of dicts. 

        """
        path = HTTPClient.urls['all_bindings']
        resp, content = self.do_call(os.path.join(self.base_url, path), 'GET')
        bindings = self.decode_json_content(content)
        return bindings



