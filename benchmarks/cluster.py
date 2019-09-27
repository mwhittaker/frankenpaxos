from . import host
from typing import Any, Callable, Dict, List
import json


# Say you have a function `connect` which converts an address (e.g., 127.0.0.1)
# into a host. Calling connect on an address more than once is wasteful. We
# don't need to connect to a host more than once. A _RemoteHostCache helps us
# avoid redundantly calling connect on the same address more than once.
class _RemoteHostCache:
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
class Cluster:
    @staticmethod
    def _sanitize_json(data: Dict[str, Any]) -> Dict[int, Dict[str, List[str]]]:
        sanitized: Dict[int, Dict[str, List[str]]] = dict()
        for (f, cluster) in data.items():
            if not isinstance(cluster, dict):
                raise ValueError(
                    f'Cluster configuration {cluster} for f value {f} is not '
                    f'an object.')

            sanitized[int(f)] = dict()
            for (role, addresses) in cluster.items():
                if not isinstance(addresses, list):
                    raise ValueError(
                        f'Addresses {addresses} for role {role} in cluster '
                        f'{cluster} for f value {f} is not a list.')

                for (i, address) in enumerate(addresses):
                    if not isinstance(address, str):
                        raise ValueError(
                            f'Address {i} {address} for role {role} in cluster '
                            f'{cluster} for f value {f} is not a string.')

                sanitized[int(f)][role] = addresses

        return sanitized

    @staticmethod
    def from_json_file(filename: str,
                       connect: Callable[[str], host.Host]) -> 'Cluster':
        with open(filename, 'r') as f:
            cluster = Cluster._sanitize_json(json.load(f))
            return Cluster(cluster, connect)

    @staticmethod
    def from_json_string(json_string: str,
                         connect: Callable[[str], host.Host]) -> 'Cluster':
        cluster = Cluster._sanitize_json(json.loads(json_string))
        return Cluster(cluster, connect)

    @staticmethod
    def from_dict(cluster: Dict[int, Dict[str, List[str]]],
                  connect: Callable[[str], host.Host]) -> 'Cluster':
        return Cluster(cluster, connect)

    def __init__(self, cluster: Dict[int, Dict[str, List[str]]],
                 connect: Callable[[str], host.Host]) -> None:
        self._cache = _RemoteHostCache(connect)
        self._cluster = cluster

    def f(self, x: int) -> Dict[str, List[host.Host]]:
        return {
            role: [self._cache.connect(a) for a in addresses
                  ] for (role, addresses) in self._cluster[x].items()
        }
