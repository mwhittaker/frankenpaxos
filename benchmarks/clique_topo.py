from mininet.cli import CLI
import time
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.nodelib import LinuxBridge
from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.topo import Topo
from typing import Dict, List, Tuple
import base64
import itertools
import pickle
import subprocess

class CliqueTopo(Topo):
    def build(self,
              hostnames: List[str],
              link_kwargs: Dict[Tuple[str, str], Dict]) -> None:
        # Create the hosts and their switches.
        hosts = dict()
        switches = dict()
        for (i, name) in enumerate(hostnames, 1):
            hosts[name] = self.addHost(name)
            switches[name] = self.addSwitch(f's{i}', cls=LinuxBridge, stp=True)
            self.addLink(hosts[name], switches[name])

        # Connect the switches together.
        for (s1, s2) in itertools.combinations(hostnames, 2):
            kwargs = link_kwargs.get((s1, s2), {})
            self.addLink(switches[s1], switches[s2], **kwargs)

topos = {'clique': CliqueTopo}

def src_port(net, src, dst) -> int:
    links = net.linksBetween(src, dst)
    assert len(links) == 1, links
    link = links[0]
    src_intf = link.intf1 if link.intf1.node == src else link.intf2
    return src.ports[src_intf]

def main() -> None:
    setLogLevel('info')
    hostnames=['h1', 'h2', 'h3']
    topo = CliqueTopo(hostnames=hostnames, link_kwargs={
        ('h1', 'h2'): {'delay': '100ms'},
        ('h1', 'h2'): {'delay': '200ms'},
        ('h2', 'h3'): {'delay': '300ms'},
        })
    net = Mininet(
        topo=topo,
        autoSetMacs=True)
        # controller=lambda n: RemoteController(n, ip='127.0.0.1', port=6633))
        # controller=lambda n: RemoteController(n, ip='127.0.0.1'))

    # nodes = {name: net.get(name) for name in hostnames}
    # switches = {name: switch for (name, switch) in zip(hostnames, net.switches)}
    # routes = {int(switch.dpid): {} for switch in switches.values()}
    # for (name1, name2) in itertools.combinations(hostnames, 2):
    #     h1 = nodes[name1]
    #     h2 = nodes[name2]
    #     s1 = switches[name1]
    #     s2 = switches[name2]
    #     routes[int(s1.dpid)][(h1.MAC(), h2.MAC())] = src_port(net, s1, s2)
    #     routes[int(s1.dpid)][(h2.MAC(), h1.MAC())] = src_port(net, s1, h1)
    #
    #     routes[int(s2.dpid)][(h2.MAC(), h1.MAC())] = src_port(net, s2, s1)
    #     routes[int(s2.dpid)][(h1.MAC(), h2.MAC())] = src_port(net, s2, h2)
    #
    # payload = base64.b64encode(pickle.dumps(routes, protocol=2)).decode("utf-8")
    # pox = subprocess.Popen([
    #     '/home/vagrant/install/pox/pox.py',
    #     'yolo',
    #     f'--data="{payload}"',
    # ])
    net.start()
    CLI(net)
    net.stop()
    # pox.terminate()

if __name__ == '__main__':
    main()
