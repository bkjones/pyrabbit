try:
    import unittest2 as unittest
except ImportError:
    import unittest

import sys
sys.path.append('..')
from pyrabbit import http



class TestHTTPClient(unittest.TestCase):
    """
    Except for the init test, these are largely functional tests that
    require a RabbitMQ management API to be available on localhost:55672

    """
    def setUp(self):
        self.c = http.HTTPClient('localhost:55672', 'guest', 'guest')

    def test_client_init(self):
        c = http.HTTPClient('localhost:55672', 'guest', 'guest')
        self.assertIsInstance(c, http.HTTPClient)

    def test_is_alive(self):
        self.assertTrue(self.c.is_alive())

    def test_is_not_alive(self):
        """
        If your vhost isn't found, RabbitMQ throws a 404. This is mapped to
        a more readable exception message in HTTPClient saying the vhost
        doesn't exist.

        """
        with self.assertRaises(http.APIError):
            self.c.is_alive('somenonexistentvhost')

    def test_overview_500(self):
        """
        Insures that if the broker is down, the pyrabbit.api.NetworkError gets
        raised.

        """
        c = http.HTTPClient('webstatuscodes.appspot.com/500',
                                    'guest', 'guest')
        
        with self.assertRaises(http.HTTPError) as ctx:
            c.get_overview()

        self.assertEqual(ctx.exception.status, 500)

    def test_get_users_200(self):
        self.assertTrue(self.c.get_users())
        self.assertIsNotNone(self.c.get_users())

    def test_get_users_noprivs(self):
        srvr = http.HTTPClient('localhost:55672', 'luser', 'luser')
        self.assertRaises(http.APIError, srvr.get_users)

    def test_get_whoami_200(self):
        self.assertTrue(self.c.get_whoami())

    def test_get_overview(self):
        overview = self.c.get_overview()
        self.assertIsInstance(overview, dict)

    def test_get_all_vhosts(self):
        vhosts = self.c.get_all_vhosts()
        self.assertIsInstance(vhosts, list)

    def test_get_all_queues(self):
        queues = self.c.get_queues()
        self.assertIsInstance(queues, list)

    def test_get_queues_for_vhost(self):
        queues = self.c.get_queues('testvhost')
        self.assertIsInstance(queues, list)

    def test_get_named_queue(self):
        queue = self.c.get_queue('%2F', 'TestQ')
        self.assertIsInstance(queue, dict)

    def test_get_all_exchanges(self):
        xchs = self.c.get_exchanges()
        self.assertIsInstance(xchs, list)

    def test_get_exchanges_by_vhost(self):
        xchs = self.c.get_exchanges('testvhost')
        self.assertIsInstance(xchs, list)

    def test_get_exchange_by_name(self):
        xch = self.c.get_exchange('%2F', 'test')
        self.assertIsInstance(xch, dict)
        self.assertEqual(xch['name'], 'test')

    def test_get_all_connections(self):
        conns = self.c.get_connections()
        self.assertIsInstance(conns, list)

    def test_get_all_channels(self):
        chans = self.c.get_channels()
        self.assertIsInstance(chans, list)

    def test_purge_queue(self):
        status = self.c.purge_queue('%2F', 'testq')
        self.assertTrue(status)
        
    def test_create_exchange(self):
        self.assertTrue(self.c.create_exchange('%2F', 'pyrabbit_test_exchange', 'direct'))

    def test_delete_exchange(self):
        self.assertTrue(self.c.delete_exchange('%2F', 'pyrabbit_test_exchange'))
