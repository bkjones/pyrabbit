==================
PyRabbit
==================

pyrabbit is a module to make it easy to interface w/ RabbitMQ's HTTP Management
API.  It's tested against RabbitMQ 2.4.1 using Python 2.6-3.2. Yes, it works
great w/ Python 3! I strive for full test coverage, and right now it has 95%
coverage of the http module, and 100% coverage of the api module.

In its current state, pyrabbit makes only one non-GET call: a DELETE call to
purge a queue. This will change very soon as I add enough functionality to
rewrite bunny (http://github.com/bkjones/bunny) using pyrabbit instead of
py-amqplib. 

There's plenty to be done, so please do jump in, fork, send pull requests and
issues, etc. 

Here's a quick demo:

    newhotness:pyrabbit bjones$ python3.2
    Python 3.2 (r32:88445, Mar 29 2011, 20:18:33) 
    [GCC 4.2.1 (Apple Inc. build 5666) (dot 3)] on darwin
    Type "help", "copyright", "credits" or "license" for more information.

    >>> from pyrabbit import api
    >>> srvr = api.Server('localhost:55672', 'guest', 'guest')
    >>> for q in srvr.get_queues():
    ...     '{0.name:<40s}{0.messages:>15d}'.format(q)
    ... 
    'amq.gen-1QXqTFany6PFsi2NSmv2qA==                      0'
    'amq.gen-KmfVj0FEIy9zBtuTtWo4Iw==                      0'
    'amq.gen-TMPMGWJgmaZtVnUa1CAavw==                      0'
    'amq.gen-TZ+dIc1riU2drT78tar/YQ==                      0'
    'amq.gen-bNZKE9TDbTFBEwlCgI6v4Q==                      0'
    'amq.gen-qluGgP91Oe6OQFpg0oWRww==                      0'
    'amq.gen-z3EzucIl0qJVCXiL6hNUsg==                      0'
    'jonesyTestQ                                           2'
    'publish                                               0'
    'testq                                                 0'
    >>> testq = srvr.get_queue('%2F', 'jonesyTestQ')
    >>> testq
    Queue(memory=12744, messages=2, consumer_details=[], idle_since='2011-5-27
    14:26:25', exclusive_consumer_pid='', exclusive_consumer_tag='',
    messages_ready=2, messages_unacknowledged=0, consumers=0,
    backing_queue_status={'q1': 0, 'q3': 2, 'q2': 0, 'q4': 0,
    'avg_ack_egress_rate': 0.0, 'ram_msg_count': 0, 'ram_ack_count': 0,
    'outstanding_txns': 0, 'len': 2, 'persistent_count': 2, 'target_ram_count': 0,
    'next_seq_id': 3, 'delta': ['delta', 'undefined', 0, 'undefined'],
    'pending_acks': 0, 'avg_ack_ingress_rate': 0.0, 'avg_egress_rate': 0.0,
    'avg_ingress_rate': 0.0, 'ram_index_count': 0}, name='jonesyTestQ',
    vhost='/', durable=True, auto_delete=False, owner_pid='none', arguments={},
    pid='<rabbit@newhotness.3.12974.633>', node='rabbit@newhotness')
    >>> srvr.purge_queues([testq])  
    True
    >>> for q in srvr.get_queues():
    ...     '{0.name:<40s}{0.messages:>15d}'.format(q)
    ... 
    'amq.gen-1QXqTFany6PFsi2NSmv2qA==                      0'
    'amq.gen-KmfVj0FEIy9zBtuTtWo4Iw==                      0'
    'amq.gen-TMPMGWJgmaZtVnUa1CAavw==                      0'
    'amq.gen-TZ+dIc1riU2drT78tar/YQ==                      0'
    'amq.gen-bNZKE9TDbTFBEwlCgI6v4Q==                      0'
    'amq.gen-qluGgP91Oe6OQFpg0oWRww==                      0'
    'amq.gen-z3EzucIl0qJVCXiL6hNUsg==                      0'
    'jonesyTestQ                                           0'
    'publish                                               0'
    'testq                                                 0'

