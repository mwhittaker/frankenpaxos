from .benchmark import BenchmarkDirectory, SuiteDirectory
from tqdm import tqdm
from .proto_util import message_to_pbtext
from enum import Enum
from mininet.net import Mininet
from mininet.topo import Topo
from subprocess import Popen
from typing import Dict, List, NamedTuple
import argparse
import csv
import mininet
import os
import pandas as pd


class Input(NamedTuple):
    f: int
    num_clients: int
    num_threads_per_client: int


class Output(NamedTuple):
    mean_latency: float
    median_latency: float
    p90_latency: float
    p95_latency: float
    p99_latency: float
    throughput: float


class RoundSystemType(Enum):
  CLASSIC_ROUND_ROBIN = 0
  ROUND_ZERO_FAST = 1
  MIXED_ROUND_ROBIN = 2


class MultiPaxosNet(object):
    def __init__(self, num_clients: int, f: int) -> None:
        self.num_clients = num_clients
        self.f = f
        self.clients: List[mininet.node.Node] = []
        self.leaders: List[mininet.node.Node] = []
        self.acceptors: List[mininet.node.Node] = []
        self.client_switch: mininet.node.Switch = None
        self.switch: mininet.node.Switch = None
        self.net = Mininet()

        # Add clients.
        self.client_switch = self.net.addSwitch('s1')
        for i in range(num_clients):
            client = self.net.addHost(f'c{i}')
            self.net.addLink(client, self.client_switch)
            self.clients.append(client)

        # Add leaders and acceptors.
        num_leaders = f + 1
        num_acceptors = 2*f + 1
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

    def __enter__(self):
        self.net.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.net.stop()

    def config(self) -> Dict:
        """Returns a MultiPaxos config.

        Note that the network must be started """
        return {
            'f': self.f,
            'leaderAddress': [
                {'host': l.IP(), 'port': 9000}
                for (i, l) in enumerate(self.leaders)
            ],
            'leaderElectionAddress': [
                {'host': l.IP(), 'port': 9001}
                for (i, l) in enumerate(self.leaders)
            ],
            'leaderHeartbeatAddress': [
                {'host': l.IP(), 'port': 9002}
                for (i, l) in enumerate(self.leaders)
            ],
            'acceptorAddress': [
                {'host': a.IP(), 'port': 10000}
                for (i, a) in enumerate(self.acceptors)
            ],
            'acceptorHeartbeatAddress': [
                {'host': a.IP(), 'port': 10001}
                for (i, a) in enumerate(self.acceptors)
            ],
            'roundSystemType': RoundSystemType.CLASSIC_ROUND_ROBIN,
    }


def read_csvs(filenames: List[str]) -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename, header=0))
    return pd.concat(dfs)

def run_benchmark(bench: BenchmarkDirectory,
                  args: argparse.Namespace,
                  input: Input) -> Output:
    with MultiPaxosNet(num_clients=input.num_clients, f=input.f) as pnet:
        # Write config file.
        config = pnet.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename, message_to_pbtext(config))

        # Launch acceptors.
        acceptors: List[Popen] = []
        for (i, host) in enumerate(pnet.acceptors):
            cmd = [
                'java',
                '-cp', os.path.abspath(args.jar),
                'frankenpaxos.fastmultipaxos.AcceptorMain',
                '--index', str(i),
                '--config', config_filename,
            ]
            bench.write_string(f'acceptor_{i}_cmd.txt', ' '.join(cmd))
            out = bench.create_file(f'acceptor_{i}_out.txt')
            err = bench.create_file(f'acceptor_{i}_err.txt')
            acceptors.append(host.popen(cmd, stdout=out, stderr=err))

        # Launch leaders.
        leaders: List[Popen] = []
        for (i, host) in enumerate(pnet.leaders):
            cmd = [
                'java',
                '-cp', os.path.abspath(args.jar),
                'frankenpaxos.fastmultipaxos.LeaderMain',
                '--index', str(i),
                '--config', config_filename,
            ]
            bench.write_string(f'leader_{i}_cmd.txt', ' '.join(cmd))
            out = bench.create_file(f'leader_{i}_out.txt')
            err = bench.create_file(f'leader_{i}_err.txt')
            leaders.append(host.popen(cmd, stdout=out, stderr=err))

        # Launch clients.
        clients: List[Popen] = []
        for (i, host) in enumerate(pnet.clients):
            cmd = [
                'java',
                '-cp', os.path.abspath(args.jar),
                'frankenpaxos.fastmultipaxos.BenchmarkClientMain',
                '--host', host.IP(),
                '--port', str(11000),
                '--config', config_filename,
                '--duration', '20s',
                '--num_threads', str(input.num_threads_per_client),
                '--output_file_prefix', bench.abspath(f'client_{i}'),
            ]
            bench.write_string(f'client_{i}_cmd.txt', ' '.join(cmd))
            out = bench.create_file(f'client_{i}_out.txt')
            err = bench.create_file(f'client_{i}_err.txt')
            clients.append(host.popen(cmd, stdout=out, stderr=err))

        # Wait for clients to finish and then terminate leaders and acceptors.
        for p in clients:
            p.wait()
        for p in leaders + acceptors:
            p.terminate()

        # Every client thread j on client i writes results to `client_i_j.csv`.
        # We concatenate these results into a single CSV file.
        df = read_csvs([bench.abspath(f'client_{i}_{j}.csv')
                        for i in range(input.num_clients)
                        for j in range(input.num_threads_per_client)])
        df.to_csv(bench.abspath('data.csv'))

        earliest_start = pd.to_datetime(df['start']).min()
        latest_stop = pd.to_datetime(df['stop']).max()
        duration = (latest_stop - earliest_start).total_seconds()
        return Output(
            mean_latency = df['latency_nanos'].mean(),
            median_latency = df['latency_nanos'].median(),
            p90_latency = df['latency_nanos'].quantile(.90),
            p95_latency = df['latency_nanos'].quantile(.95),
            p99_latency = df['latency_nanos'].quantile(.99),
            throughput = df.shape[0] / duration
        )


def main(args) -> None:
    with SuiteDirectory(args.suite_directory, 'multipaxos') as suite:
        print(f'Running benchmark suite in {suite.path}.')
        suite.write_dict('args.json', vars(args))
        results_file = suite.create_file('results.csv')
        results_writer = csv.writer(results_file)
        results_writer.writerow(Input._fields + Output._fields)

        inputs = [
            Input(f=f,
                  num_clients=num_clients,
                  num_threads_per_client=num_threads_per_client)
            for f in [1]
            for num_clients in range(1, 3)
            for num_threads_per_client in range(1, 6)
        ]
        for input in tqdm(inputs):
            with suite.benchmark_directory() as bench:
                bench.write_string('input.txt', str(input))
                bench.write_dict('input.json', input._asdict())
                output = run_benchmark(bench, args, input)
                row = [str(x) for x in list(input) + list(output)]
                results_writer.writerow(row)
                results_file.flush()

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
