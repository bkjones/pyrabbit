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
