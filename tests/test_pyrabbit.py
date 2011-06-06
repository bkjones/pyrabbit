from collections import namedtuple
import json

try:
    #python 2.x
    import unittest2 as unittest
except ImportError:
    #python 3.x
    import unittest

import sys
sys.path.append('..')
import pyrabbit
from pyrabbit import http
import mock

# used by Server init
test_overview_dict = {'node': 'bar', 
            'management_version': '2.4.1',
            'queue_totals': 'rrrr',
            'listeners': 'ssss',
            'statistics_db_node': 'tttt',
            'message_stats': 'uuuu',
            'statistics_level': 'vvvv'}

test_vhosts_dict = [{'name': '/'}]

test_q_all = json.loads('[{"memory":12248,"messages":0,"consumer_details":[],"idle_since":"2011-5-27 15:17:35","exclusive_consumer_pid":"","exclusive_consumer_tag":"","messages_ready":0,"messages_unacknowledged":0,"messages":0,"consumers":0,"backing_queue_status":{"q1":0,"q2":0,"delta":["delta","undefined",0,"undefined"],"q3":0,"q4":0,"len":0,"pending_acks":0,"outstanding_txns":0,"target_ram_count":"infinity","ram_msg_count":0,"ram_ack_count":0,"ram_index_count":0,"next_seq_id":0,"persistent_count":0,"avg_ingress_rate":0.0,"avg_egress_rate":0.0,"avg_ack_ingress_rate":0.0,"avg_ack_egress_rate":0.0},"name":"testq","vhost":"/","durable":true,"auto_delete":false,"owner_pid":"none","arguments":{},"pid":"<rabbit@newhotness.3.225.0>","node":"rabbit@newhotness"}]')

class TestServer(unittest.TestCase):
    def setUp(self):
        """
        Since these are unit tests, we isolate the Server class by mocking
        out pyrabbit.api.HTTPClient and all of the methods needed for testing.

        pyrabbit.api.HTTPClient is a mock *class*, which gets instantiated by
        Server as self.client, but for convenience I've provided the
        instantiated object as self.http for the tests to use, to eliminate
        long calls like:

        pyrabbit.api.HTTPClient.return_value.get_overview.return_value = blah

        ...which becomes...

        self.http.get_overview.return_value = blah

        """
        http.HTTPClient = mock.Mock(spec_set=http.HTTPClient)
        http.HTTPClient.return_value.get_overview.return_value = test_overview_dict
        self.http = http.HTTPClient.return_value
        self.srvr = pyrabbit.api.Server('localhost:55672', 'guest', 'guest')

    def test_server_init_200(self):
        self.assertIsInstance(self.srvr, pyrabbit.api.Server)
        self.assertEqual(self.srvr.host, 'localhost:55672')

    def test_server_is_alive_default_vhost(self):
        self.http.is_alive.return_value = True
        self.assertTrue(self.srvr.is_alive())

    def test_get_vhosts_200(self):
        self.http.get_all_vhosts.return_value = test_vhosts_dict
        vhosts = self.srvr.get_all_vhosts()
        self.assertIsInstance(vhosts, list)

    def test_get_all_queues(self):
        self.http.get_queues.return_value = test_q_all
        queues = self.srvr.get_queues()
        self.assertIsInstance(queues, list)

    def test_purge_queues(self):
        self.http.purge_queue.return_value = True
        qu = namedtuple("Queue", ['name', 'vhost'])
        q1 = qu(name='q1', vhost='%2F')
        q2 = qu(name='q2', vhost='%2F')
        self.assertTrue(self.srvr.purge_queues([q1, q2]))

    def test_get_all_exchanges(self):
        xchs = [{'name': 'foo', 'vhost': '/', 'type': 'direct',
                 'durable': False, 'auto_delete': False, 'internal': False,
                 'arguments': {}},

                {'name': 'bar', 'vhost': '/', 'type': 'direct',
                 'durable': False, 'auto_delete': False, 'internal': False,
                 'arguments': {}},]
        self.http.get_exchanges.return_value = xchs
        xlist = self.srvr.get_exchanges()
        self.assertIsInstance(xlist, list)
        self.assertEqual(len(xlist), 2)

    def test_get_named_exchange(self):
        xch = {'name': 'foo', 'vhost': '/', 'type': 'direct',
                 'durable': False, 'auto_delete': False, 'internal': False,
                 'arguments': {}}
        self.http.get_exchange.return_value = xch
        myexch = self.srvr.get_exchange('%2F', 'foo')
        self.assertEqual(myexch.name, 'foo')

    def test_get_users_noprivs(self):
        with mock.patch.object(pyrabbit.api.Server, 'has_admin_rights') as mock_rights:
            mock_rights.__get__ = mock.Mock(return_value=False)
            self.assertRaises(pyrabbit.api.PermissionError, self.srvr.get_users)

    def test_get_users_withprivs(self):
        with mock.patch.object(pyrabbit.api.Server, 'has_admin_rights') as mock_rights:
            mock_rights.__get__ = mock.Mock(return_value=True)
            self.assertTrue(self.srvr.get_users)

