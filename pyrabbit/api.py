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
        Returns a list of dictionaries.

        """
        users = self.client.get_users()
        return users

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
        return vhosts

    def get_queues(self, vhost=None):
        """
        Get all queues, or all queues in a vhost if vhost is not None.
        Returns a list.

        """
        queues = self.client.get_queues(vhost)
        return queues

    def get_queue(self, vhost, name):
        """
        Get a single queue, which requires both vhost and name.

        """
        vhost = '%2F' if vhost == '/' else vhost
        q = self.client.get_queue(vhost, name)
        return q

    def get_queue_depth(self, vhost, name):
        vhost = '%2F' if vhost == '/' else vhost
        q = self.get_queue(vhost, name)
        depth = q['messages']
        return depth

    def purge_queues(self, queues):
        """
        The queues parameter should be a list of tuples of the format:

        ('name', 'vhost')

        If 'vhost' == '/', it'll be changed to '%2F'

        """
        for name, vhost in queues:
            vhost = '%2F' if vhost =='/' else vhost
            self.client.purge_queue(vhost, name)
        return True

    def purge_queue(self, vhost, name):
        vhost = '%2F' if vhost == '/' else vhost
        self.client.purge_queue(vhost, name)

    def get_connections(self, name=None):
        """
        Returns a list of one or more dictionaries

        """
        connections = self.client.get_connections(name)
        return connections

    def get_exchanges(self, vhost=None):
        """
        Returns a list of dictionaries.

        """
        xchs = self.client.get_exchanges(vhost)
        return xchs

    def get_exchange(self, vhost, xname):
        """
        Returns a single exchange namedtuple subclass.

        """
        xch = self.client.get_exchange(vhost, xname)
        return xch

    def is_alive(self, vhost='%2F'):
        return self.client.is_alive(vhost)


