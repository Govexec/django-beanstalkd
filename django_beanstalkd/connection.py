import beanstalkc
from django.conf import settings

from .errors import BeanstalkError


def connect_beanstalkd(server=None, port=11300):
    """Connect to beanstalkd server(s) from settings file"""

    if server is None:
        server = getattr(settings, 'BEANSTALK_SERVER', '127.0.0.1')

    if server.find(':') > -1:
        server, port = server.split(':', 1)

    try:
        port = int(port)
        return beanstalkc.Connection(server, port)
    except (ValueError, beanstalkc.SocketError), e:
        raise BeanstalkError(e)
