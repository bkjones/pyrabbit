import unittest
import sys
sys.path.append('..')
import pyrabbit

class TestServer(unittest.TestCase):
    def test_server_init(self):
        srvr = pyrabbit.api.Server('localhost')
        self.assertIsInstance(srvr, pyrabbit.api.Server)
        self.assertEqual(srvr.host, 'localhost')

class TestExchange(unittest.TestCase):
    def test_exch_init(self):
        xch = pyrabbit.exchanges.Exchange('test')
        self.assertIsInstance(xch, pyrabbit.exchanges.Exchange)
        self.assertEqual(xch.name, 'test')
    
class TestQueue(unittest.TestCase):
    def test_queue_init(self):
        qu = pyrabbit.queues.Queue('testq')
        self.assertIsInstance(qu, pyrabbit.queues.Queue)
        self.assertEqual(qu.name, 'testq')

