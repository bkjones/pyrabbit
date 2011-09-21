from collections import namedtuple
from . import http
import functools
import time

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


class Server(object):
    """
    Abstraction of the RabbitMQ Management HTTP API.

    HTTP calls are delegated to the  HTTPClient class for ease of testing,
    cleanliness, separation of duty, flexibility, etc.
    """

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
        self.client = http.HTTPClient(self.host, self.user, self.passwd)

        # initialize this now. @needs_admin_privs will check this first to
        # avoid making an HTTP call. If this is None, it'll trigger an
        # HTTP call (by calling self.has_admin_rights) and populate this for
        # next time.
        self.is_admin = None
        return

    @property
    def has_admin_rights(self):
        """
        Determine if the creds passed in for authentication have admin
        rights to RabbitMQ data. If not, then there's a decent amount of
        information you can't get at.

        """
        whoami = self.client.get_whoami()
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
        overview = self.client.get_overview()
        return overview

    @needs_admin_privs
    def get_users(self):
        """
        :returns: a list of dictionaries, each representing a user. This 
            method is decorated with '@needs_admin_privs', and will raise
            an error if the credentials used to set up the broker connection 
            do not have admin privileges.

        """
        users = self.client.get_users()
        return users

    def get_all_vhosts(self):
        """
        :returns: a list of dicts, each dict representing a vhost 
            on the broker. 

        """
        vhosts = self.client.get_all_vhosts()
        return vhosts

    def get_queues(self, vhost=None):
        """
        Get all queues, or all queues in a vhost if vhost is not None.
        Returns a list.

        :param string vhost: The virtual host to list queues for. If This is None
                   (the default), all queues for the broker instance are returned. 
        :returns: A list of dicts, each representing a queue.
        :rtype: list of dicts

        """
        queues = self.client.get_queues(vhost)
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
        q = self.client.get_queue(vhost, name)
        return q

    def get_queue_depth(self, vhost, name):
        """
        Get the number of messages currently in a queue. This is a convenience
         function that just calls :meth:`Server.get_queue` and pulls out/returns the 'messages' 
         field from the dictionary it returns. 

        :param string vhost: The vhost of the queue being queried.
        :param string name: The name of the queue to query. 
        :returns: Number of messages in the queue
        :rtype: integer

        """
        vhost = '%2F' if vhost == '/' else vhost
        q = self.get_queue(vhost, name)
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
            self.client.purge_queue(vhost, name)
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
        self.client.purge_queue(vhost, name)
        return 

    def get_connections(self, name=None):
        """
        Returns a list of one or more dictionaries, each holding 
        attributes of a connection to the broker. If 'name' is given and
        not None, returns a dictionary for a named connection. 

        :param string name: The name of a connection to query for. 
        :returns: A list of dictionaries, each representing a connection.

        """
        connections = self.client.get_connections(name)
        return connections

    def get_exchanges(self, vhost=None):
        """
        :param string vhost: The vhost to list exchanges from.
        :returns: a list of dictionaries.

        """
        xchs = self.client.get_exchanges(vhost)
        return xchs

    def get_exchange(self, vhost, xname):
        """
        :param string vhost: Name of the vhost the exchange belongs to.
        :param string xname: Name of the exchange to query for. 
        :returns: A dictionary w/ the exchange's attributes.
        :rtype: dict

        """
        xch = self.client.get_exchange(vhost, xname)
        return xch

    def is_alive(self, vhost='%2F'):
        return self.client.is_alive(vhost)


