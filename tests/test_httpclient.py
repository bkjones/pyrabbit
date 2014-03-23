try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock
import httplib2
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
        domain = ''
        expected_credentials = [(domain, self.testuser, self.testpass)]
        self.assertEqual(
            self.c.client.credentials.credentials, expected_credentials)

    def test_client_init_sets_default_timeout(self):
        self.assertEqual(self.c.client.timeout, 5)

    def test_client_init_with_timeout(self):
        c = http.HTTPClient(self.testhost, self.testuser, self.testpass, 1)
        self.assertEqual(c.client.timeout, 1)

    def test_base_url(self):
        self.assertEquals('http://localhost:55672/api/', self.c.base_url)

    @mock.patch('httplib2.Http', spec=httplib2.Http)
    def test_url_path_creation(self, httplib):
        client = mock.Mock()
        httplib.return_value = client
        c = http.HTTPClient('localhost:55672', 'guest', 'guest')
        resp = mock.Mock()
        resp.status = 200
        client.request.return_value = (resp, None)
        c.do_call('queues', 'GET')
        client.request.assert_called_once_with(
            'http://localhost:55672/api/queues',
            'GET', 
            None,
            None
        )
