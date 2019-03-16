from contextlib import contextmanager
from mininet.net import Mininet

@contextmanager
def managed_net(net: Mininet):
    net.start()
    yield net
    net.stop()
