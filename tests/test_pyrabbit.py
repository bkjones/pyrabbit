from collections import namedtuple

try:
    #python 2.x
    import unittest2 as unittest
except ImportError:
    #python 3.x
    import unittest

import sys
sys.path.append('..')
import pyrabbit
from mock import Mock, patch

class TestClient(unittest.TestCase):
    def setUp(self):
        self.client = pyrabbit.api.Client('localhost:55672', 'guest', 'guest')

    def test_server_init_200(self):
        self.assertIsInstance(self.client, pyrabbit.api.Client)
        self.assertEqual(self.client.host, 'localhost:55672')

    def test_server_is_alive_default_vhost(self):
        Response = namedtuple('Response', ['status'])
        resp = Response(status=200)
        self.client.http.do_call = Mock(return_value=resp)
        self.assertTrue(self.client.is_alive())

    def test_get_vhosts_200(self):
        self.client.http.do_call = Mock(return_value=[])
        vhosts = self.client.get_all_vhosts()
        self.assertIsInstance(vhosts, list)

    def test_get_all_queues(self):
        self.client.http.do_call = Mock(return_value=[])
        queues = self.client.get_queues()
        self.assertIsInstance(queues, list)

    def test_purge_queues(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.purge_queues(['q1', 'q2']))

    def test_get_all_exchanges(self):
        xchs = [{'name': 'foo', 'vhost': '/', 'type': 'direct',
                 'durable': False, 'auto_delete': False, 'internal': False,
                 'arguments': {}},

                {'name': 'bar', 'vhost': '/', 'type': 'direct',
                 'durable': False, 'auto_delete': False, 'internal': False,
                 'arguments': {}},]
        self.client.http.do_call = Mock(return_value=xchs)
        xlist = self.client.get_exchanges()
        self.assertIsInstance(xlist, list)
        self.assertEqual(len(xlist), 2)

    def test_get_named_exchange(self):
        xch = {'name': 'foo', 'vhost': '/', 'type': 'direct',
                 'durable': False, 'auto_delete': False, 'internal': False,
                 'arguments': {}}
        self.client.http.do_call = Mock(return_value=xch)
        myexch = self.client.get_exchange('%2F', 'foo')
        self.assertEqual(myexch['name'], 'foo')

    def test_get_users_noprivs(self):
        with patch.object(pyrabbit.api.Client, 'has_admin_rights') as mock_rights:
            mock_rights.__get__ = Mock(return_value=False)
            self.assertRaises(pyrabbit.api.PermissionError, self.client.get_users)

    def test_get_users_withprivs(self):
        with patch.object(pyrabbit.api.Client, 'has_admin_rights') as mock_rights:
            mock_rights.__get__ = Mock(return_value=True)
            self.assertTrue(self.client.get_users)

    def test_get_queue_depth(self):
        q = {'messages': 4}
        self.client.http.do_call = Mock(return_value=q)
        depth = self.client.get_queue_depth('/', 'test')
        self.assertEqual(depth, q['messages'])

    def test_purge_queue(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.purge_queue('vname', 'qname'))

    def test_create_queue(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.create_queue('qname', 'vname'))

    def test_get_connections(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_connections())

    def test_get_connection(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_connection('cname'))

    def test_get_channels(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_channels())

    def test_get_bindings(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_bindings())

    def test_create_binding(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.create_binding('vhost',
                                                   'exch',
                                                   'queue',
                                                   'rt_key'))

    def test_publish(self):
        self.client.http.do_call = Mock(return_value={'routed': 'true'})
        self.assertTrue(self.client.publish('vhost', 'xname', 'rt_key',
                                            'payload'))

if __name__ == "__main__":
    log = open('test_out.log', 'w')
    unittest.main(testRunner=unittest.TextTestRunner(log))
