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
    require a RabbitMQ management API to be available on localhost

    """
    testhost = 'localhost:15672'
    testuser = 'guest'
    testpass = 'guest'
    def setUp(self):
        self.c = http.HTTPClient(self.testhost, self.testuser, self.testpass)

    def test_client_init(self):
        c = http.HTTPClient(self.testhost, self.testuser, self.testpass)
        self.assertIsInstance(c, http.HTTPClient)

    def test_client_init_sets_credentials(self):
        self.assertEqual(self.c.auth.username, self.testuser)
        self.assertEqual(self.c.auth.password, self.testpass)

    def test_client_init_sets_default_timeout(self):
        self.assertEqual(self.c.timeout, 5)

    def test_client_init_with_timeout(self):
        c = http.HTTPClient(self.testhost, self.testuser, self.testpass, 1)
        self.assertEqual(c.timeout, 1)

