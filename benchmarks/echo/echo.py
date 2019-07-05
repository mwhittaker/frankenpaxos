from .. import benchmark
from .. import parser_util
from .. import pd_util
from .. import prometheus
from mininet.net import Mininet
from typing import Any, Callable, Collection, Dict, List, NamedTuple
import argparse
import csv
import enum
import mininet
import os
import pandas as pd
import subprocess
import time
import tqdm
import yaml


class Input(NamedTuple):
    duration_seconds: float
    timeout_seconds: float
    net_name: str
    num_client_procs: int
    num_clients_per_proc: int
    profiled: bool
    monitored: bool
    prometheus_scrape_interval_ms: int


Output = benchmark.RecorderOutput


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

    def clients(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def server(self) -> mininet.node.Node:
        raise NotImplementedError()


class SingleSwitchNet(EchoNet):
    def __init__(self, num_client_procs: int) -> None:
        self._clients: List[mininet.node.Node] = []
        self._server: mininet.node.Node = None
        self._net = Mininet()

        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        for i in range(num_client_procs):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(client)

        self._server = self._net.addHost(f'h1')
        self._net.addLink(self._server, switch)

    def net(self) -> Mininet:
        return self._net

    def clients(self) -> List[mininet.node.Node]:
        return self._clients

    def server(self) -> mininet.node.Node:
        return self._server


class EchoSuite(benchmark.Suite[Input, Output]):
    def _run_benchmark(self,
                       bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any],
                       input: Input,
                       net: EchoNet) -> Output:
        # Launch server.
        server_proc = bench.popen(
            f=net.server().popen,
            label='server',
            cmd = [
                'java',
                '-cp', os.path.abspath(args['jar']),
                'frankenpaxos.echo.BenchmarkServerMain',
                '--host', net.server().IP(),
                '--port', '9000',
                '--prometheus_host', net.server().IP(),
                '--prometheus_port', '9001',
            ],
            profile=input.profiled
        )
        bench.log('Servers started.')

        # Launch Prometheus, and give it some time to start.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                input.prometheus_scrape_interval_ms,
                {'echo_server': [f'{net.server().IP()}:9001']}
            )
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                f=net.server().popen,
                label='prometheus',
                cmd = [
                    'prometheus',
                    f'--config.file={bench.abspath("prometheus.yml")}',
                    f'--storage.tsdb.path={bench.abspath("prometheus_data")}',
                ],
            )

        # Wait for server and prometheus.
        bench.log('Waiting.')
        time.sleep(5)
        bench.log('Waiting over.')

        # Launch clients.
        client_procs = []
        for (i, host) in enumerate(net.clients()):
            client_proc = bench.popen(
                f=net.server().popen,
                label=f'client_{i}',
                cmd = [
                    'timeout', f'{input.timeout_seconds}s',
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.echo.BenchmarkClientMain',
                    '--server_host', net.server().IP(),
                    '--server_port', '9000',
                    '--host', host.IP(),
                    '--port', '10000',
                    '--duration', f'{input.duration_seconds}s',
                    '--num_clients', f'{input.num_clients_per_proc}',
                    '--output_file', bench.abspath(f'client_{i}_data.csv'),
                ]
            )
            client_procs.append(client_proc)
        bench.log('Clients started.')

        # Wait for experiment to finish.
        for client_proc in client_procs:
            client_proc.wait()
        server_proc.terminate()
        if input.monitored:
            prometheus_server.terminate()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`. We concatenate these
        # results into a single CSV file.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        return benchmark.parse_recorder_data(bench, client_csvs)

    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        with SingleSwitchNet(num_client_procs=input.num_client_procs) as net:
            return self._run_benchmark(bench, args, input, net)


def _main(args) -> None:
    class ExampleEchoSuite(EchoSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    duration_seconds=15,
                    timeout_seconds=30,
                    net_name='SingleSwitchNet',
                    num_client_procs=num_client_procs,
                    num_clients_per_proc=1,
                    profiled=args.profile,
                    monitored=args.monitor,
                    prometheus_scrape_interval_ms=200,
                )
                for num_client_procs in [1, 2, 3]
            ] * 2

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_client_procs': input.num_client_procs,
                'output.stop_throughput_1s.p90':
                    f'{output.stop_throughput_1s.p90}',
            })

    suite = ExampleEchoSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'echo') as dir:
        suite.run_suite(dir)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()


if __name__ == '__main__':
    _main(get_parser().parse_args())
