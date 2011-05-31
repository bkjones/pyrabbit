class Server(object):
    """
    Abstraction of the API which acts as a proxy for 
    requests going to a RabbitMQ server.

    Pretty much the entirety of the supported API for 
    pyrabbit is accessed through this class.

    """
    def __init__(self, host):
        self.host = host
