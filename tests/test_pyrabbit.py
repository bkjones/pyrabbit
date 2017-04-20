"""Main test file for the pyrabbit Client."""

import json

try:
    #python 2.x
    import unittest2 as unittest
except ImportError:
    #python 3.x
    import unittest

import sys
import requests
sys.path.append('..')
import pyrabbit
from mock import Mock, patch


class TestClient(unittest.TestCase):
    def setUp(self):
        self.client = pyrabbit.api.Client('localhost:55672/api', 'guest', 'guest')

    def tearDown(self):
        del self.client

    def test_server_init_200(self):
        self.assertIsInstance(self.client, pyrabbit.api.Client)
        self.assertEqual(self.client.api_url, 'localhost:55672/api')

    def test_server_is_alive_default_vhost(self):
        response = {'status': 'ok'}
        self.client.http.do_call = Mock(return_value=response)
        self.assertTrue(self.client.is_alive())

    def test_get_vhosts_200(self):
        self.client.http.do_call = Mock(return_value=[])
        vhosts = self.client.get_all_vhosts()
        self.assertIsInstance(vhosts, list)

    def test_get_all_queues(self):
        self.client.http.do_call = Mock(return_value=[])
        queues = self.client.get_queues()
        self.assertIsInstance(queues, list)

    def test_get_nodes(self):
        self.client.http.do_call = Mock(return_value=[])
        nodes = self.client.get_nodes()
        self.assertIsInstance(nodes, list)

    def test_purge_queues(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.purge_queues(['q1', 'q2']))

    def test_get_queue(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_queue('', 'q1'))

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

    def test_get_users(self):
        with patch('pyrabbit.http.HTTPClient.do_call') as do_call:
            self.assertTrue(self.client.get_users())
    
    def test_get_definitions(self):
        with patch('pyrabbit.http.HTTPClient.do_call') as do_call:
            self.assertTrue(self.client.get_definitions())

    def test_get_queue_depth(self):
        q = {'messages': 4}
        self.client.http.do_call = Mock(return_value=q)
        depth = self.client.get_queue_depth('/', 'test')
        self.assertEqual(depth, q['messages'])

    def test_get_queue_depth_2(self):
        """
        An integration test that includes the HTTP client's do_call
        method and json decoding operations.

        """
        q = {'messages': 8}
        json_q = json.dumps(q)

        with patch('requests.request') as req:
            resp = requests.Response()
            resp._content = json_q.encode()
            resp.status_code = 200
            req.return_value = resp
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

    def test_delete_connection(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.delete_connection('127.0.0.1:1234 -> 127.0.0.1:5678 (1)'))

    def test_get_channels(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_channels())

    def test_get_channel(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_channel('127.0.0.1:1234 -> 127.0.0.1:5678 (1)'))

    def test_get_bindings(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_bindings())

    def test_create_binding(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.create_binding('vhost',
                                                   'exch',
                                                   'queue',
                                                   'rt_key'))

    def test_delete_binding(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.delete_binding('vhost',
                                                   'exch',
                                                   'queue',
                                                   'rt_key'))

    def test_publish(self):
        self.client.http.do_call = Mock(return_value={'routed': 'true'})
        self.assertTrue(self.client.publish('vhost', 'xname', 'rt_key',
                                            'payload'))

    def test_create_vhost(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.create_vhost('vname'))

    def test_delete_vhost(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.delete_vhost('vname'))

    def test_create_user(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.create_user('user', 'password'))

    def test_delete_user(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.delete_user('user'))

    def test_get_permissions(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_permissions())

    def test_get_vhost_permissions(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_vhost_permissions('vname'))

    def test_get_user_permissions(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_user_permissions('username'))

    def test_delete_permission(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.delete_permission('vname', 'username'))

    def test_get_permission(self):
        self.client.http.do_call = Mock(return_value=True)
        self.assertTrue(self.client.get_permission('vname', 'username'))

    def test_is_alive(self):
        with patch('pyrabbit.http.HTTPClient.do_call') as do_call:
            do_call.return_value = {'status': 'ok'}
            self.assertTrue(self.client.is_alive())


@unittest.skip
class TestLiveServer(unittest.TestCase):
    def setUp(self):
        self.rabbit = pyrabbit.api.Client('localhost:15672', 'guest', 'guest')
        self.vhost_name = 'pyrabbit_test_vhost'
        self.exchange_name = 'pyrabbit_test_exchange'
        self.queue_name = 'pyrabbit_test_queue'
        self.rt_key = 'pyrabbit-roundtrip'
        self.payload = 'pyrabbit test message payload'
        self.user = 'guest'

    def test_round_trip(self):
        """
        This does a 'round trip' test, which consists of the following steps:

        * Create a vhost, and verify creation
        * Give 'guest' all perms on vhost
        * Create an exchange in that vhost, verify creation
        * Create a queue
        * Create a binding between the queue and exchange
        * Publish a message to the exchange that makes it to the queue
        * Grab that message from the queue (verify it's the same message)
        * Delete binding and verify we don't receive messages
        * Delete the exchange
        * Delete the vhost
        """

        # create a vhost, verify creation, and grant all perms to 'guest'.
        self.rabbit.create_vhost(self.vhost_name)
        vhosts = [i['name'] for i in self.rabbit.get_all_vhosts()]
        self.assertIn(self.vhost_name, vhosts)
        self.rabbit.set_vhost_permissions(self.vhost_name, self.user,
                                          '.*', '.*', '.*')

        # create an exchange, and verify creation.
        self.rabbit.create_exchange(self.vhost_name,
                                    self.exchange_name,
                                    'direct')
        self.assertEqual(self.exchange_name,
                         self.rabbit.get_exchange(self.vhost_name,
                                                  self.exchange_name)['name'])

        # create a queue and verify it was created
        self.rabbit.create_queue(self.vhost_name,self.queue_name)
        self.assertEqual(self.queue_name,
                        self.rabbit.get_queue(self.vhost_name,
                                              self.queue_name)['name'])

        # bind the queue and exchange
        self.rabbit.create_binding(self.vhost_name, self.exchange_name,
                                   self.queue_name, self.rt_key)

        # publish a message, and verify by getting it back.
        self.rabbit.publish(self.vhost_name, self.exchange_name, self.rt_key,
                            self.payload)
        messages = self.rabbit.get_messages(self.vhost_name, self.queue_name)
        self.assertEqual(messages[0]['payload'], self.payload)

        # delete binding and verify we don't get the message
        self.rabbit.delete_binding(self.vhost_name, self.exchange_name,
                                   self.queue_name, self.rt_key)
        self.rabbit.publish(self.vhost_name, self.exchange_name, self.rt_key,
                            self.payload)
        messages = self.rabbit.get_messages(self.vhost_name, self.queue_name)
        self.assertIsNone(messages)

        # Clean up.
        self.rabbit.delete_exchange(self.vhost_name, self.exchange_name)
        self.rabbit.delete_vhost(self.vhost_name)


if __name__ == "__main__":
    log = open('test_out.log', 'w')
    unittest.main(testRunner=unittest.TextTestRunner(log))
