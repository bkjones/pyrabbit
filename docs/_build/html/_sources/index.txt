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

    newhotness:pyrabbit bjones$ python3.2
    Python 3.2 (r32:88445, Mar 29 2011, 20:18:33) 
    [GCC 4.2.1 (Apple Inc. build 5666) (dot 3)] on darwin
    Type "help", "copyright", "credits" or "license" for more information.

    >>> from pyrabbit import api
    >>> srvr = api.Server('localhost:55672', 'guest', 'guest')
    >>> for q in srvr.get_queues():
    ...     '{0:<40s}{1:>15d}'.format(q['name'], q['messages'])
    ... 
    'TestQ                                                 1'
    'aliveness-test                                        0'
    'testq                                                 0'
    'test123                                               0'
    >>> testq = srvr.get_queue('/', 'TestQ')
    >>> testq
    {'node': 'rabbit@newhotness', 'consumer_details': [], 'name': 'TestQ', 'consumers': 0, 'backing_queue_status': {'q1': 0, 'q3': 0, 'q2': 0, 'q4': 1, 'avg_ack_egress_rate': 0.0, 'ram_msg_count': 1, 'ram_ack_count': 0, 'outstanding_txns': 0, 'len': 1, 'persistent_count': 1, 'target_ram_count': 'infinity', 'next_seq_id': 1, 'delta': ['delta', 'undefined', 0, 'undefined'], 'pending_acks': 0, 'avg_ack_ingress_rate': 0.0, 'avg_egress_rate': 0.0, 'avg_ingress_rate': 2.50361099907801e-05, 'ram_index_count': 0}, 'pid': '<rabbit@newhotness.3.229.0>', 'durable': True, 'messages': 1, 'idle_since': '2011-9-21 8:58:32', 'vhost': '/', 'owner_pid': 'none', 'auto_delete': False, 'memory': 14680, 'exclusive_consumer_tag': '', 'exclusive_consumer_pid': '', 'messages_unacknowledged': 0, 'messages_ready': 1, 'arguments': {}}
