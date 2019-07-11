from .. import benchmark
from .. import host
from .. import parser_util
from .. import pd_util
from .. import proc
from .. import prometheus
from typing import Any, Callable, Collection, Dict, List, NamedTuple
import argparse
import csv
import datetime
import enum
import itertools
import mininet
import mininet.net
import os
import pandas as pd
import paramiko
import subprocess
import time
import tqdm
import yaml


# Input/Output #################################################################
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


# Networks #####################################################################
class EchoNet(object):
    def __enter__(self) -> 'EchoNet':
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        pass

    def clients(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def server(self) -> host.Endpoint:
        raise NotImplementedError()


class RemoteEchoNet(EchoNet):
    def __init__(self, addresses: List[str], num_client_procs: int) -> None:
        assert len(addresses) > 0
        self.hosts = [self._make_host(a) for a in addresses]
        self.num_client_procs = num_client_procs

    def _make_host(self, address: str) -> host.Host:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
        client.connect(address)
        return host.RemoteHost(client)

    def clients(self) -> List[host.Endpoint]:
        if len(self.hosts) == 1:
            hosts = self.hosts * self.num_client_procs
        else:
            cycle = itertools.cycle(self.hosts[1:])
            hosts = list(itertools.islice(cycle, self.num_client_procs))
        return [host.Endpoint(h, 10000 + 100*i) for (i, h) in enumerate(hosts)]

    def server(self) -> host.Endpoint:
        return host.Endpoint(self.hosts[0], 9000)


class EchoMininet(EchoNet):
    def __enter__(self) -> 'EchoNet':
        self.net().start()
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        self.net().stop()

    def net(self) -> mininet.net.Mininet:
        raise NotImplementedError()


class SingleSwitchMininet(EchoMininet):
    def __init__(self, num_client_procs: int) -> None:
        self._net = mininet.net.Mininet()
        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        self._clients: List[host.Endpoint] = []
        for i in range(num_client_procs):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(host.Endpoint(host.MininetHost(client), 10000))

        server = self._net.addHost(f'h1')
        self._net.addLink(server, switch)
        self._server = host.Endpoint(host.MininetHost(server), 9000)

    def net(self) -> mininet.net.Mininet:
        return self._net

    def clients(self) -> List[host.Endpoint]:
        return self._clients

    def server(self) -> host.Endpoint:
        return self._server


# Suite ########################################################################
class EchoSuite(benchmark.Suite[Input, Output]):
    def make_net(self, args: Dict[Any, Any], input: Input) -> EchoNet:
        raise NotImplementedError

    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        with self.make_net(args, input) as net:
            return self._run_benchmark(bench, args, input, net)

    def _run_benchmark(self,
                       bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any],
                       input: Input,
                       net: EchoNet) -> Output:
        # Launch server.
        server_proc = bench.popen(
            host=net.server().host,
            label='server',
            cmd = [
                'java',
                '-cp', os.path.abspath(args['jar']),
                'frankenpaxos.echo.BenchmarkServerMain',
                '--host', net.server().host.ip(),
                '--port', str(net.server().port),
                '--prometheus_host', net.server().host.ip(),
                '--prometheus_port',
                    str(net.server().port + 1) if input.monitored else '-1',
            ]
        )
        bench.log('Servers started.')

        # Launch Prometheus, and give it some time to start.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                input.prometheus_scrape_interval_ms,
                {'echo_server':
                 [f'{net.server().host.ip()}:{net.server().port + 1}']}
            )
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.server().host,
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
        for (i, client) in enumerate(net.clients()):
            client_proc = bench.popen(
                host=net.server().host,
                label=f'client_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.echo.BenchmarkClientMain',
                    '--server_host', net.server().host.ip(),
                    '--server_port', str(net.server().port),
                    '--host', client.host.ip(),
                    '--port', str(client.port),
                    '--duration', f'{input.duration_seconds}s',
                    '--timeout', f'{input.timeout_seconds}s',
                    '--num_clients', f'{input.num_clients_per_proc}',
                    '--output_file', bench.abspath(f'client_{i}_data.csv'),
                ]
            )
            client_procs.append(client_proc)
        bench.log('Clients started.')

        # Wait for experiment to finish.
        for client_proc in client_procs:
            client_proc.wait()
        server_proc.kill()
        if input.monitored:
            prometheus_server.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`. We concatenate these
        # results into a single CSV file.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        return benchmark.parse_recorder_data(bench, client_csvs,
                drop_prefix=datetime.timedelta(seconds=0))


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
