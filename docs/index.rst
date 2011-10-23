.. PyRabbit documentation master file, created by
   sphinx-quickstart on Tue Sep 20 22:48:33 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to PyRabbit's documentation!
====================================

Contents:

.. toctree::
   :maxdepth: 2

   api
   http

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

==================
PyRabbit Overview
==================

pyrabbit is a module to make it easy to interface w/ RabbitMQ's HTTP Management
API.  It's tested against RabbitMQ 2.4.1 using Python 2.6-3.2. Yes, it works
great w/ Python 3!

Here's a quick demo::

    >>> from pyrabbit.api import Client
    >>> cl = Client('localhost:55672', 'guest', 'guest')
    >>> cl.is_alive()
    True
    >>> cl.create_vhost('example_vhost')
    True
    >>> [i['name'] for i in cl.get_all_vhosts()]
    [u'/', u'diabolica', u'example_vhost', u'testvhost']
    >>> cl.get_vhost_names()
    [u'/', u'diabolica', u'example_vhost', u'testvhost']
    >>> cl.set_vhost_permissions('example_vhost', 'guest', '.*', '.*', '.*')
    True
    >>> cl.create_exchange('example_vhost', 'example_exchange', 'direct')
    True
    >>> cl.get_exchange('example_vhost', 'example_exchange')
    {u'name': u'example_exchange', u'durable': True, u'vhost': u'example_vhost', u'internal': False, u'arguments': {}, u'type': u'direct', u'auto_delete': False}
    >>> cl.create_queue('example_queue', 'example_vhost')
    True
    >>> cl.create_binding('example_vhost', 'example_exchange', 'example_queue', 'my.rtkey')
    True
    >>> cl.publish('example_vhost', 'example_exchange', 'my.rtkey', 'example message payload')
    True
    >>> cl.get_messages('example_vhost', 'example_queue')
    [{u'payload': u'example message payload', u'exchange': u'example_exchange', u'routing_key': u'my.rtkey', u'payload_bytes': 23, u'message_count': 2, u'payload_encoding': u'string', u'redelivered': False, u'properties': []}]
    >>> cl.delete_vhost('example_vhost')
    True
    >>> [i['name'] for i in cl.get_all_vhosts()]
    [u'/', u'diabolica', u'testvhost']

