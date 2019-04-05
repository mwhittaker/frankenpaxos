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
from typing import Callable, Dict, List, NamedTuple
import argparse
import csv
import os
import pandas as pd
import subprocess
import time

class Input(NamedTuple):
    net_name: str
    num_clients: int
    num_threads_per_client: int
    duration_seconds: float
    profiled: bool


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


class EchoNet(object):
    def __init__(self) -> None:
        pass

    def __enter__(self) -> 'EchoNet':
        self.net().start()
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        self.net().stop()

    def net(self) -> Mininet:
        raise NotImplementedError()

    def clients(self) -> List[Node]:
        raise NotImplementedError()

    def server(self) -> Node:
        raise NotImplementedError()

class SingleSwitchNet(EchoNet):
    def __init__(self, num_clients: int) -> None:
        self._clients: List[Node] = []
        self._server: Node = None
        self._net = Mininet()

        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        for i in range(num_clients):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(client)

        self._server = self._net.addHost(f'h1')
        self._net.addLink(self._server, switch)

    def net(self) -> Mininet:
        return self._net

    def clients(self) -> List[Node]:
        return self._clients

    def server(self) -> Node:
        return self._server

def run_benchmark(bench: BenchmarkDirectory,
                  args: argparse.Namespace,
                  input: Input,
                  net: EchoNet) -> Output:
    # Launch server.
    server_proc = bench.popen(
        f=net.server().popen,
        label='server',
        cmd = [
            'java',
            '-cp', os.path.abspath(args.jar),
            'frankenpaxos.echo.BenchmarkServerMain',
            '--host', net.server().IP(),
            '--port', '9000',
        ],
        profile=args.profile
    )

    # Launch clients.
    client_procs = []
    for (i, host) in enumerate(net.clients()):
        client_proc = bench.popen(
            f=net.server().popen,
            label=f'client_{i}',
            cmd = [
                'java',
                '-cp', os.path.abspath(args.jar),
                'frankenpaxos.echo.BenchmarkClientMain',
                '--server_host', net.server().IP(),
                '--server_port', '9000',
                '--host', host.IP(),
                '--port', '10000',
                '--duration', f'{input.duration_seconds}s',
                '--num_threads', str(input.num_threads_per_client),
                '--output_file_prefix', bench.abspath(f'client_{i}'),
            ]
        )
        client_procs.append(client_proc)

    # Wait for clients to finish.
    for client_proc in client_procs:
        client_proc.wait()

    # Kill server.
    server_proc.terminate()

    # Every client thread j on client i writes results to `client_i_j.csv`.  We
    # concatenate these results into a single CSV file.
    client_csvs = [bench.abspath(f'client_{i}_{j}.csv')
                   for i in range(input.num_clients)
                   for j in range(input.num_threads_per_client)]
    df = read_csvs(client_csvs)
    df['start'] = pd.to_datetime(df['start'])
    df['stop'] = pd.to_datetime(df['stop'])
    df = df.set_index('start').sort_index(0)
    df['throughput_1s'] = util.throughput(df, 1000)
    df['throughput_2s'] = util.throughput(df, 2000)
    df['throughput_5s'] = util.throughput(df, 5000)
    df.to_csv(bench.abspath('data.csv'))

    # Since we concatenate and save the file, we can throw away the originals.
    for client_csv in client_csvs:
        os.remove(client_csv)

    # We also compress the output data since it can get big.
    subprocess.call(['gzip', bench.abspath('data.csv')])

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

MakeNet = Callable[[argparse.Namespace, List[Input]], EchoNet]

def run_suite(args: argparse.Namespace,
              inputs: List[Input],
              make_net: Callable[[Input], EchoNet]) -> None:
    with SuiteDirectory(args.suite_directory, 'echo') as suite:
        print(f'Running benchmark suite in {suite.path}.')
        suite.write_dict('args.json', vars(args))
        suite.write_string('inputs.txt', '\n'.join(str(i) for i in inputs))

        results_file = suite.create_file('results.csv')
        results_writer = csv.writer(results_file)
        results_writer.writerow(Input._fields + Output._fields)

        for input in tqdm(inputs):
            with suite.benchmark_directory() as bench:
                with make_net(input) as net:
                    bench.write_string('input.txt', str(input))
                    bench.write_dict('input.json', input._asdict())
                    output = run_benchmark(bench, args, input, net)
                    row = [str(x) for x in list(input) + list(output)]
                    results_writer.writerow(row)
                    results_file.flush()


def _main(args) -> None:
    inputs = [
        Input(
            net_name='SingleSwitchNet',
            num_clients=num_clients,
            num_threads_per_client=num_threads_per_client,
            duration_seconds=duration_seconds,
            profiled=args.profile,
        )
        for num_clients in [1]
        for num_threads_per_client in [1, 2, 3]
        for duration_seconds in [10]
    ]

    def make_net(input) -> EchoNet:
        return SingleSwitchNet(num_clients=input.num_clients)

    run_suite(args, inputs, make_net)


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
    parser.add_argument(
        '-p', '--profile',
        action='store_true',
        help='Profile code'
    )
    return parser


if __name__ == '__main__':
    _main(get_parser().parse_args())
