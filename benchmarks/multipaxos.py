from .benchmark import SuiteDirectory
from .mininet_util import managed_net
from .proto_util import message_to_pbtext
from enum import Enum
from mininet.net import Mininet
from mininet.topo import Topo
from subprocess import Popen
from typing import List
import argparse
import mininet
import os

class RoundSystemType(Enum):
  CLASSIC_ROUND_ROBIN = 0
  ROUND_ZERO_FAST = 1
  MIXED_ROUND_ROBIN = 2

class MultiPaxosNet(object):
    def __init__(self, num_clients: int, f: int) -> None:
        self.net = Mininet()
        self.clients: List[mininet.node.Node] = []
        self.leaders: List[mininet.node.Node] = []
        self.acceptors: List[mininet.node.Node] = []
        self.client_switch: mininet.node.Switch = None
        self.switch: mininet.node.Switch = None

        num_leaders = f + 1
        num_acceptors = 2*f + 1

        # Add clients.
        self.client_switch = self.net.addSwitch('s1')
        for i in range(num_clients):
            client = self.net.addHost(f'c{i}')
            self.net.addLink(client, self.client_switch)
            self.clients.append(client)

        # Add leaders and acceptors.
        self.switch = self.net.addSwitch('s2')
        for i in range(num_leaders):
            leader = self.net.addHost(f'l{i}')
            self.net.addLink(leader, self.switch)
            self.leaders.append(leader)
        for i in range(num_acceptors):
            acceptor = self.net.addHost(f'a{i}')
            self.net.addLink(acceptor, self.switch)
            self.acceptors.append(acceptor)

        # Connect the two switches.
        self.net.addLink(self.client_switch, self.switch)

        # Add controller.
        self.net.addController('c')

def main(args) -> None:
    with SuiteDirectory(args.suite_directory, 'multipaxos') as suite:
        suite.write_dict('args.json', vars(args))

        mininet.log.setLogLevel('info')
        num_clients = 1
        f = 1
        paxos_net = MultiPaxosNet(num_clients=num_clients, f=f)
        with managed_net(paxos_net.net) as net:
            config = {
                'f': f,
                'leaderAddress': [
                    {'host': l.IP(), 'port': 9000 + i*100}
                    for (i, l) in enumerate(paxos_net.leaders)
                ],
                'leaderElectionAddress': [
                    {'host': l.IP(), 'port': 9000 + i*100 + 1}
                    for (i, l) in enumerate(paxos_net.leaders)
                ],
                'leaderHeartbeatAddress': [
                    {'host': l.IP(), 'port': 9000 + i*100 + 2}
                    for (i, l) in enumerate(paxos_net.leaders)
                ],
                'acceptorAddress': [
                    {'host': a.IP(), 'port': 10000 + i*100}
                    for (i, a) in enumerate(paxos_net.acceptors)
                ],
                'acceptorHeartbeatAddress': [
                    {'host': a.IP(), 'port': 10000 + i*100 + 1}
                    for (i, a) in enumerate(paxos_net.acceptors)
                ],
                'roundSystemType': RoundSystemType.CLASSIC_ROUND_ROBIN,
            }
            config_filename = suite.write_string('config.pbtxt',
                                                 message_to_pbtext(config))

            with suite.benchmark_directory() as bench:
                acceptors: List[Popen] = []
                for (i, host) in enumerate(paxos_net.acceptors):
                    cmd = [
                        'java', '-cp', os.path.abspath(args.jar),
                        'frankenpaxos.fastmultipaxos.AcceptorMain',
                        '--index', str(i),
                        '--config', config_filename,
                    ]
                    bench.write_string(f'acceptor_{i}_cmd.txt', ' '.join(cmd))
                    out = bench.create_file(f'acceptor_{i}_out.txt')
                    err = bench.create_file(f'acceptor_{i}_err.txt')
                    acceptors.append(host.popen(cmd, stdout=out, stderr=err))

                leaders: List[Popen] = []
                for (i, host) in enumerate(paxos_net.leaders):
                    cmd = [
                        'java', '-cp', os.path.abspath(args.jar),
                        'frankenpaxos.fastmultipaxos.LeaderMain',
                        '--index', str(i),
                        '--config', config_filename,
                    ]
                    bench.write_string(f'leader_{i}_cmd.txt', ' '.join(cmd))
                    out = bench.create_file(f'leader_{i}_out.txt')
                    err = bench.create_file(f'leader_{i}_err.txt')
                    leaders.append(host.popen(cmd, stdout=out, stderr=err))

                clients: List[Popen] = []
                for (i, host) in enumerate(paxos_net.clients):
                    cmd = [
                        'java', '-cp', os.path.abspath(args.jar),
                        'frankenpaxos.fastmultipaxos.BenchmarkClientMain',
                        '--host', host.IP(),
                        '--port', str(11000),
                        '--config', config_filename,
                        '--duration', '20s',
                        '--num_threads', '2',
                        '--output_file_prefix', bench.abspath(f'client_{i}'),
                    ]
                    bench.write_string(f'client_{i}_cmd.txt', ' '.join(cmd))
                    out = bench.create_file(f'client_{i}_out.txt')
                    err = bench.create_file(f'client_{i}_err.txt')
                    clients.append(host.popen(cmd, stdout=out, stderr=err))

                for p in clients:
                    p.wait()
                for p in leaders + acceptors:
                    p.terminate()

                # mininet.cli.CLI(net)

def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--suite_directory',
        type=str,
        required=True,
        help='Benchmark suite directory'
    )
    parser.add_argument(
        '-j', '--jar',
        type=str,
        required=True,
        help='FrankenPaxos JAR file'
    )
    return parser

if __name__ == '__main__':
    main(get_parser().parse_args())
