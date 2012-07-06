"""Main test file for the pyrabbit Client."""

import json

try:
    #python 2.x
    import unittest2 as unittest
except ImportError:
    #python 3.x
    import unittest

try:
    #python 2.x
    from contextlib2 import contextmanager, ExitStack
except ImportError:
    #python 3.x
    from contextlib import contextmanager, ExitStack

import sys
sys.path.append('..')
import pyrabbit
from pyrabbit.http import HTTPError
from pyrabbit.api import APIError
from mock import Mock, patch

@contextmanager
def patches(*args):
    stack = ExitStack()
    for thing in args:
        name = thing.split('.')[-1]
        new_patch = patch(thing)
        triggered_patch = stack.enter_context(new_patch)
        setattr(stack, name, triggered_patch)

    yield stack


class BaseClient(unittest.TestCase):

    def setUp(self):
        self.configure()
        self.execute()

    def configure(self):
        self.faker = patches('pyrabbit.api.http.HTTPClient')
        self.host = 'localhost:55672'
        self.user = 'guest'
        self.passwd = 'guest'

    def execute(self):
        with self.faker as self.phony:
            self.client = pyrabbit.api.Client(self.host,
                                              self.user,
                                              self.passwd)


####
##
## Client.__init__
##
####

class DescribeClientInit(BaseClient):

    def should_set_user_supplied_host(self):
        self.assertEqual(self.client.host, self.host)

    def should_set_user_supplied_user(self):
        self.assertEqual(self.client.user, self.user)

    def should_set_user_supplied_passwd(self):
        self.assertEqual(self.client.passwd, self.passwd)

    def should_instantiate_HTTPClient(self):
        self.phony.HTTPClient.assert_called_once_with(self.host,
                                                 self.user,
                                                 self.passwd)

    def should_initialize_is_admin_to_None(self):
        self.assertIsNone(self.client.is_admin)

####
##
## Client.is_alive
##
####

class _BaseIsAlive(unittest.TestCase):

    def setUp(self):
        self.configure()
        with self.patcher as self.patched:
            self.setup_patches()
            self.execute()

    def configure(self):
        self.exc = None
        self.monkeypatches = [
            'pyrabbit.api.Client.build_url',
            'pyrabbit.api.needs_admin_privs',
            'pyrabbit.api.http.HTTPClient',
        ]
        self.patcher = patches(*self.monkeypatches)

        self.vhost = Mock(name='vhost')
        self.host = Mock(name='host')
        self.user = Mock(name='user')
        self.passwd = Mock(name='passwd')
        self.client = pyrabbit.api.Client(
            self.host,
            self.user,
            self.passwd
        )

    def setup_patches(self):
        self.client.http = self.patched.HTTPClient()
        self.client.build_url = self.patched.build_url
        self.client.build_url.return_value = '/%s' % self.vhost
        self.client.is_admin = True


    def execute(self):
        try:
            self.resp = self.client.is_alive(self.vhost)
        except Exception as exc:
            self.exc = exc

    def should_construct_live_test_url(self):
        self.client.build_url.assert_called_once_with('live_test', self.vhost)

    def should_call_live_test_uri(self):
        self.client.http.do_call.assert_called_with(
            '/%s' % self.vhost,
            'GET',
        )


class WhenIsAliveResponseStatusIsOK(_BaseIsAlive):

    def setup_patches(self):
        super(WhenIsAliveResponseStatusIsOK, self).setup_patches()
        self.client.http.do_call.return_value = {'status': 'ok'}

    def should_return_true(self):
        self.assertIs(self.resp, True)


class WhenIsAliveResponseStatusIsNotOK(_BaseIsAlive):
    def setup_patches(self):
        super(WhenIsAliveResponseStatusIsNotOK, self).setup_patches()
        self.client.http.do_call.return_value = {'status': 'No Bueno'}

    def should_return_false(self):
        self.assertIs(self.resp, False)


class WhenIsAliveThrows404(_BaseIsAlive):
    def setup_patches(self):
        super(WhenIsAliveThrows404, self).setup_patches()
        self.client.http.do_call.side_effect = HTTPError({}, status=404)

    def should_raise_APIError(self):
        self.assertIsInstance(self.exc, APIError)

#class TestClient(unittest.TestCase):
#    def setUp(self):
#        self.client = pyrabbit.api.Client('localhost:55672', 'guest', 'guest')
#
#    def tearDown(self):
#        del self.client
#
#    def test_server_init_200(self):
#        self.assertIsInstance(self.client, pyrabbit.api.Client)
#        self.assertEqual(self.client.host, 'localhost:55672')
#
#    def test_server_is_alive_default_vhost(self):
#        response = {'status': 'ok'}
#        self.client.http.do_call = Mock(return_value=response)
#        with patch.object(pyrabbit.api.Client, 'has_admin_rights') as mock_rights:
#            mock_rights.__get__ = Mock(return_value=True)
#            self.assertTrue(self.client.is_alive())
#
#    def test_get_vhosts_200(self):
#        self.client.http.do_call = Mock(return_value=[])
#        vhosts = self.client.get_all_vhosts()
#        self.assertIsInstance(vhosts, list)
#
#    def test_get_all_queues(self):
#        self.client.http.do_call = Mock(return_value=[])
#        queues = self.client.get_queues()
#        self.assertIsInstance(queues, list)
#
#    def test_purge_queues(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.purge_queues(['q1', 'q2']))
#
#    def test_get_all_exchanges(self):
#        xchs = [{'name': 'foo', 'vhost': '/', 'type': 'direct',
#                 'durable': False, 'auto_delete': False, 'internal': False,
#                 'arguments': {}},
#
#                {'name': 'bar', 'vhost': '/', 'type': 'direct',
#                 'durable': False, 'auto_delete': False, 'internal': False,
#                 'arguments': {}},]
#        self.client.http.do_call = Mock(return_value=xchs)
#        xlist = self.client.get_exchanges()
#        self.assertIsInstance(xlist, list)
#        self.assertEqual(len(xlist), 2)
#
#    def test_get_named_exchange(self):
#        xch = {'name': 'foo', 'vhost': '/', 'type': 'direct',
#                 'durable': False, 'auto_delete': False, 'internal': False,
#                 'arguments': {}}
#        self.client.http.do_call = Mock(return_value=xch)
#        myexch = self.client.get_exchange('%2F', 'foo')
#        self.assertEqual(myexch['name'], 'foo')
#
#    @patch.object(pyrabbit.api.Client, 'has_admin_rights')
#    def test_get_users_noprivs(self, has_rights):
#        has_rights.__get__ = Mock(return_value=False)
#        self.assertRaises(pyrabbit.api.PermissionError, self.client.get_users)
#
#    @patch.object(pyrabbit.api.Client, 'has_admin_rights')
#    def test_get_users_withprivs(self, has_rights):
#        has_rights.return_value = True
#        with patch('pyrabbit.http.HTTPClient.do_call') as do_call:
#            self.assertTrue(self.client.get_users())
#
#    def test_get_queue_depth(self):
#        q = {'messages': 4}
#        self.client.http.do_call = Mock(return_value=q)
#        depth = self.client.get_queue_depth('/', 'test')
#        self.assertEqual(depth, q['messages'])
#
#    def test_get_queue_depth_2(self):
#        """
#        An integration test that includes the HTTP client's do_call
#        method and json decoding operations.
#
#        """
#        q = {'messages': 8}
#        json_q = json.dumps(q)
#
#        with patch('httplib2.Response') as resp:
#            resp.reason = 'response reason here'
#            resp.status = 200
#            self.client.http.client.request = Mock(return_value=(resp, json_q))
#            depth = self.client.get_queue_depth('/', 'test')
#            self.assertEqual(depth, q['messages'])
#
#    def test_purge_queue(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.purge_queue('vname', 'qname'))
#
#    def test_create_queue(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.create_queue('qname', 'vname'))
#
#    def test_get_connections(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.get_connections())
#
#    def test_get_connection(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.get_connection('cname'))
#
#    def test_get_channels(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.get_channels())
#
#    def test_get_bindings(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.get_bindings())
#
#    def test_create_binding(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.create_binding('vhost',
#                                                   'exch',
#                                                   'queue',
#                                                   'rt_key'))
#
#    def test_publish(self):
#        self.client.http.do_call = Mock(return_value={'routed': 'true'})
#        self.assertTrue(self.client.publish('vhost', 'xname', 'rt_key',
#                                            'payload'))
#
#    def test_create_vhost(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.create_vhost('vname'))
#
#    def test_delete_vhost(self):
#        self.client.http.do_call = Mock(return_value=True)
#        self.assertTrue(self.client.delete_vhost('vname'))
#
#    @patch.object(pyrabbit.api.Client, 'has_admin_rights')
#    def test_is_alive_withprivs(self, mock_rights):
#        mock_rights.__get__ = Mock(return_value=True)
#        with patch('pyrabbit.http.HTTPClient.do_call') as do_call:
#            do_call.return_value = {'status': 'ok'}
#            self.assertTrue(self.client.is_alive())
#
#    def test_is_alive_noprivs(self):
#        with patch.object(pyrabbit.api.Client, 'has_admin_rights') as mock_rights:
#            mock_rights.__get__ = Mock(return_value=False)
#            self.assertRaises(pyrabbit.api.PermissionError, self.client.is_alive)
#
#
@unittest.skip
class TestLiveServer(unittest.TestCase):
    def setUp(self):
        self.rabbit = pyrabbit.api.Client('localhost:55672', 'guest', 'guest')
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
        self.rabbit.create_queue(self.queue_name, self.vhost_name)
        self.assertEqual(self.queue_name,
                        self.rabbit.get_queue(self.vhost_name,
                                              self.queue_name)['name'])

        # bind the queue and exchange
        self.rabbit.create_binding(self.vhost_name, self.exchange_name,
                                   self.queue_name, self.rt_key)

        # publish a message, and verify by get'ing it back.
        self.rabbit.publish(self.vhost_name, self.exchange_name, self.rt_key,
                            self.payload)
        messages = self.rabbit.get_messages(self.vhost_name, self.queue_name)
        self.assertEqual(messages[0]['payload'], self.payload)

        # Clean up.
        self.rabbit.delete_exchange(self.vhost_name, self.exchange_name)
        self.rabbit.delete_vhost(self.vhost_name)


#if __name__ == "__main__":
#    log = open('test_out.log', 'w')
#    unittest.main(testRunner=unittest.TextTestRunner(log))