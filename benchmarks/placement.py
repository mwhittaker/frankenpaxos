from . import host
from typing import Any, Callable, Dict, List
import json

# Say you have a function `connect` which converts an address (e.g., 127.0.0.1)
# into a host. Calling connect on an address more than once is wasteful. We
# don't need to connect to a host more than once. A RemoteHostCache helps us
# avoid redundantly calling connect on the same address more than once.
class RemoteHostCache:
    def __init__(self, connect: Callable[[str], host.Host]) -> None:
        self._connect = connect
        self._hosts: Dict[str, host.Host] = dict()

    def connect(self, address: str) -> host.Host:
        if address in self._hosts:
            return self._hosts[address]
        else:
            host = self._connect(address)
            self._hosts[address] = host
            return host

# Say you want to run Paxos. You need a set of acceptors, a set of replicas, a
# set of leaders, and so on. Moreover, the number of each depends on the
# parameter f. We can write this in a json file that looks like this:
#
#   {
#     '1': {
#       'leaders': ['127.0.0.1', '127.0.0.1'],
#       'acceptors': ['127.0.0.1', '127.0.0.1', '127.0.0.1'],
#       'replicas': ['127.0.0.1', '127.0.0.1'],
#       'clients': ['127.0.0.1', '127.0.0.1'],
#     },
#     '2': {
#       'leaders': ['127.0.0.1', '127.0.0.1', '127.0.0.1'],
#       'acceptors': ['127.0.0.1', '127.0.0.1', '127.0.0.1', '127.0.0.1'],
#       'replicas': ['127.0.0.1', '127.0.0.1'],
#       'clients': ['127.0.0.1', '127.0.0.1'],
#     },
#   }
#
# Placement helps us deal with this kind of data.
class Placement:
    def __init__(self,
                 json_placement: Dict[str, Any],
                 connect: Callable[[str], host.Host]) -> None:
        self._cache = RemoteHostCache(connect)
        self._json_placement = json_placement

    def f(self, x: int) -> Dict[str, List[host.Host]]:
        return {
            role: [self._cache.connect(a) for a in addresses]
            for (role, addresses) in self._json_placement[str(x)].items()
        }
