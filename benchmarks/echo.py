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

class Input(NamedTuple):
    net_name: str
    num_clients: int
    num_threads_per_client: int
    duration_seconds: float


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
    # We want to ensure that all processes are always killed.
    with ExitStack() as stack:
        # Launch server.
        cmd = [
            'java',
            '-cp', os.path.abspath(args.jar),
            'frankenpaxos.echo.BenchmarkServerMain',
            '--host', net.server().IP(),
            '--port', '9000',
        ]
        bench.write_string(f'server_cmd.txt', ' '.join(cmd))
        out = bench.create_file(f'server_out.txt')
        err = bench.create_file(f'server_err.txt')
        server_proc = net.server().popen(cmd, stdout=out, stderr=err)
        stack.enter_context(Reaped(server_proc))

        # Launch leaders.
        client_procs: List[Popen] = []
        for (i, host) in enumerate(net.clients()):
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
            bench.write_string(f'client_{i}_cmd.txt', ' '.join(cmd))
            out = bench.create_file(f'client_{i}_out.txt')
            err = bench.create_file(f'client_{i}_err.txt')
            proc = host.popen(cmd, stdout=out, stderr=err)
            stack.enter_context(Reaped(proc))
            client_procs.append(proc)

        # Wait for clients to finish and then terminate server.
        for proc in client_procs:
            proc.wait()
        server_proc.terminate()

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
    with SuiteDirectory(args.suite_directory, 'echo') as suite:
        print(f'Running benchmark suite in {suite.path}.')
        suite.write_dict('args.json', vars(args))
        results_file = suite.create_file('results.csv')
        results_writer = csv.writer(results_file)
        results_writer.writerow(Input._fields + Output._fields)

        inputs = [
            Input(net_name='SingleSwitchNet',
                  num_clients=num_clients,
                  num_threads_per_client=num_threads_per_client,
                  duration_seconds=30)
            for num_clients in range(1, 4)
            for num_threads_per_client in range(1, 5)
        ] * 3
        for input in tqdm(inputs):
            with suite.benchmark_directory() as bench:
                with SingleSwitchNet(num_clients=input.num_clients) as net:
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
