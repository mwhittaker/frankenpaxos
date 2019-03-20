from . import proto_util
from . import util
from .benchmark import BenchmarkDirectory, SuiteDirectory
from .util import read_csvs, Reaped
from contextlib import ExitStack
from enum import Enum
from mininet.net import Mininet
from mininet.node import Node
from subprocess import Popen
from tqdm import tqdm
from typing import Dict, List, NamedTuple
import argparse
import csv
import os
import pandas as pd
import time


class RoundSystemType(Enum):
  CLASSIC_ROUND_ROBIN = 0
  ROUND_ZERO_FAST = 1
  MIXED_ROUND_ROBIN = 2


class Input(NamedTuple):
    # System-wide parameters.
    net_name: str
    f: int
    num_clients: int
    num_threads_per_client: int
    round_system_type: str

    # Benchmark parameters.
    duration_seconds: float
    client_lag_seconds: float

    # Client parameters.
    client_repropose_period_seconds: float


class Output(NamedTuple):
    mean_latency: float
    median_latency: float
    p90_latency: float
    p95_latency: float
    p99_latency: float

    mean_1_second_throughput: float
    median_1_second_throughput: float
    p90_1_second_throughput: float
    p95_1_second_throughput: float
    p99_1_second_throughput: float

    mean_2_second_throughput: float
    median_2_second_throughput: float
    p90_2_second_throughput: float
    p95_2_second_throughput: float
    p99_2_second_throughput: float

    mean_5_second_throughput: float
    median_5_second_throughput: float
    p90_5_second_throughput: float
    p95_5_second_throughput: float
    p99_5_second_throughput: float


class FastMultiPaxosNet(object):
    def __init__(self) -> None:
        pass

    def __enter__(self) -> 'FastMultiPaxosNet':
        self.net().start()
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        self.net().stop()

    def net(self) -> Mininet:
        raise NotImplementedError()

    def clients(self) -> List[Node]:
        raise NotImplementedError()

    def leaders(self) -> List[Node]:
        raise NotImplementedError()

    def acceptors(self) -> List[Node]:
        raise NotImplementedError()

    def config(self) -> proto_util.Message:
        raise NotImplementedError()


class SingleSwitchNet(FastMultiPaxosNet):
    def __init__(self,
                 f: int,
                 num_clients: int,
                 rs_type: RoundSystemType) -> None:
        self.f = f
        self.rs_type = rs_type
        self._clients: List[Node] = []
        self._leaders: List[Node] = []
        self._acceptors: List[Node] = []
        self._net = Mininet()

        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        for i in range(num_clients):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(client)

        num_leaders = f + 1
        for i in range(num_leaders):
            leader = self._net.addHost(f'l{i}')
            self._net.addLink(leader, switch)
            self._leaders.append(leader)

        num_acceptors = 2*f + 1
        for i in range(num_acceptors):
            acceptor = self._net.addHost(f'a{i}')
            self._net.addLink(acceptor, switch)
            self._acceptors.append(acceptor)

    def net(self) -> Mininet:
        return self._net

    def clients(self) -> List[Node]:
        return self._clients

    def leaders(self) -> List[Node]:
        return self._leaders

    def acceptors(self) -> List[Node]:
        return self._acceptors

    def config(self) -> proto_util.Message:
        return {
            'f': self.f,
            'leaderAddress': [
                {'host': l.IP(), 'port': 9000}
                for (i, l) in enumerate(self.leaders())
            ],
            'leaderElectionAddress': [
                {'host': l.IP(), 'port': 9001}
                for (i, l) in enumerate(self.leaders())
            ],
            'leaderHeartbeatAddress': [
                {'host': l.IP(), 'port': 9002}
                for (i, l) in enumerate(self.leaders())
            ],
            'acceptorAddress': [
                {'host': a.IP(), 'port': 10000}
                for (i, a) in enumerate(self.acceptors())
            ],
            'acceptorHeartbeatAddress': [
                {'host': a.IP(), 'port': 10001}
                for (i, a) in enumerate(self.acceptors())
            ],
            'roundSystemType': self.rs_type
        }


def run_benchmark(bench: BenchmarkDirectory,
                  args: argparse.Namespace,
                  input: Input,
                  net: FastMultiPaxosNet) -> Output:
    # Write config file.
    config_filename = bench.abspath('config.pbtxt')
    bench.write_string(config_filename,
                       proto_util.message_to_pbtext(net.config()))

    # We want to ensure that all processes are always killed.
    with ExitStack() as stack:
        # Launch acceptors.
        acceptors: List[Popen] = []
        for (i, host) in enumerate(net.acceptors()):
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
            p = host.popen(cmd, stdout=out, stderr=err)
            stack.enter_context(Reaped(p))
            acceptors.append(p)

        # Launch leaders.
        leaders: List[Popen] = []
        for (i, host) in enumerate(net.leaders()):
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
            p = host.popen(cmd, stdout=out, stderr=err)
            stack.enter_context(Reaped(p))
            leaders.append(p)

        # Wait a bit so that a stable leader can elect itself. If we start
        # clients too soon, they may not talk to a stable leader.
        time.sleep(input.client_lag_seconds)

        # Launch clients.
        clients: List[Popen] = []
        for (i, host) in enumerate(net.clients()):
            cmd = [
                'java',
                '-cp', os.path.abspath(args.jar),
                'frankenpaxos.fastmultipaxos.BenchmarkClientMain',
                '--host', host.IP(),
                '--port', str(11000),
                '--config', config_filename,
                '--repropose_period',
                    f'{input.client_repropose_period_seconds}s',
                '--duration', f'{input.duration_seconds}s',
                '--num_threads', str(input.num_threads_per_client),
                '--output_file_prefix', bench.abspath(f'client_{i}'),
            ]
            bench.write_string(f'client_{i}_cmd.txt', ' '.join(cmd))
            out = bench.create_file(f'client_{i}_out.txt')
            err = bench.create_file(f'client_{i}_err.txt')
            p = host.popen(cmd, stdout=out, stderr=err)
            stack.enter_context(Reaped(p))
            clients.append(p)

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
        df['start'] = pd.to_datetime(df['start'])
        df['stop'] = pd.to_datetime(df['stop'])
        df = df.set_index('start').sort_index(0)
        df['throughput_1s'] = util.throughput(df, 1000)
        df['throughput_2s'] = util.throughput(df, 2000)
        df['throughput_5s'] = util.throughput(df, 5000)
        df.to_csv(bench.abspath('data.csv'))

        return Output(
            mean_latency = df['latency_nanos'].mean(),
            median_latency = df['latency_nanos'].median(),
            p90_latency = df['latency_nanos'].quantile(.90),
            p95_latency = df['latency_nanos'].quantile(.95),
            p99_latency = df['latency_nanos'].quantile(.99),

            mean_1_second_throughput = df['throughput_1s'].mean(),
            median_1_second_throughput = df['throughput_1s'].median(),
            p90_1_second_throughput = df['throughput_1s'].quantile(.90),
            p95_1_second_throughput = df['throughput_1s'].quantile(.95),
            p99_1_second_throughput = df['throughput_1s'].quantile(.99),

            mean_2_second_throughput = df['throughput_2s'].mean(),
            median_2_second_throughput = df['throughput_2s'].median(),
            p90_2_second_throughput = df['throughput_2s'].quantile(.90),
            p95_2_second_throughput = df['throughput_2s'].quantile(.95),
            p99_2_second_throughput = df['throughput_2s'].quantile(.99),

            mean_5_second_throughput = df['throughput_5s'].mean(),
            median_5_second_throughput = df['throughput_5s'].median(),
            p90_5_second_throughput = df['throughput_5s'].quantile(.90),
            p95_5_second_throughput = df['throughput_5s'].quantile(.95),
            p99_5_second_throughput = df['throughput_5s'].quantile(.99),
        )


def _main(args) -> None:
    with SuiteDirectory(args.suite_directory, 'fastmultipaxos') as suite:
        print(f'Running benchmark suite in {suite.path}.')
        suite.write_dict('args.json', vars(args))
        results_file = suite.create_file('results.csv')
        results_writer = csv.writer(results_file)
        results_writer.writerow(Input._fields + Output._fields)

        inputs = [
            Input(net_name='SingleSwitchNet',
                f=1,
                num_clients=1,
                num_threads_per_client=1,
                round_system_type=RoundSystemType.CLASSIC_ROUND_ROBIN.name,
                duration_seconds=15,
                client_lag_seconds=3,
                client_repropose_period_seconds=10,
            )
        ] * 3
        for input in tqdm(inputs):
            with suite.benchmark_directory() as bench:
                with SingleSwitchNet(
                        f=input.f,
                        num_clients=input.num_clients,
                        rs_type = RoundSystemType[input.round_system_type]
                     ) as net:
                    bench.write_string('input.txt', str(input))
                    bench.write_dict('input.json', input._asdict())
                    output = run_benchmark(bench, args, input, net)
                    row = [str(x) for x in list(input) + list(output)]
                    results_writer.writerow(row)
                    results_file.flush()


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-s', '--suite_directory',
        type=str,
        default='/tmp',
        help='Benchmark suite directory'
    )
    parser.add_argument(
        '-j', '--jar',
        type=str,
        default = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            '..',
            'jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar'
        ),
        help='FrankenPaxos JAR file'
    )
    return parser


if __name__ == '__main__':
    _main(get_parser().parse_args())
