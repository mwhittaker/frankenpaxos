from .. import benchmark
from .. import cluster
from .. import host
from .. import parser_util
from .. import pd_util
from .. import perf_util
from .. import proc
from .. import prometheus
from .. import proto_util
from .. import util
from .. import workload
from ..workload import Workload
from typing import Any, Callable, Collection, Dict, List, NamedTuple, Optional
import argparse
import csv
import datetime
import enum
import enum
import itertools
import os
import pandas as pd
import paramiko
import subprocess
import time
import tqdm
import yaml


# Input/Output #################################################################
class ClientOptions(NamedTuple):
    pass


class ServerOptions(NamedTuple):
    flush_every_n: int = 1


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    jvm_heap_size: str

    # Benchmark parameters. ####################################################
    measurement_group_size: int
    warmup_duration: datetime.timedelta
    warmup_timeout: datetime.timedelta
    warmup_sleep: datetime.timedelta
    duration: datetime.timedelta
    timeout: datetime.timedelta
    client_lag: datetime.timedelta
    state_machine: str
    workload: Workload
    profiled: bool
    monitored: bool
    prometheus_scrape_interval: datetime.timedelta

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str

    # Server options. ##########################################################
    server_options: ServerOptions
    server_log_level: str


Output = benchmark.RecorderOutput


# Networks #####################################################################
class UnreplicatedNet:
    def __init__(self, cluster: cluster.Cluster, input: Input) -> None:
        self._cluster = cluster.f(1)
        self._input = input

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        server: host.Endpoint

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)

        def portify_one(h: host.Host) -> host.Endpoint:
            return host.Endpoint(h, next(ports))

        def portify(hosts: List[host.Host]) -> List[host.Endpoint]:
            return [portify_one(h) for h in hosts]

        def cycle_take_n(n: int, hosts: List[host.Host]) -> List[host.Host]:
            return list(itertools.islice(itertools.cycle(hosts), n))

        return self.Placement(
            clients=portify(
                cycle_take_n(self._input.num_client_procs,
                             self._cluster['clients'])),
            server=portify_one(self._cluster['server'][0]),
        )


# Suite ########################################################################
class UnreplicatedSuite(benchmark.Suite[Input, Output]):
    def __init__(self) -> None:
        super().__init__()
        self._cluster = cluster.Cluster.from_json_file(self.args()['cluster'],
                                                       self._connect)

    def _connect(self, address: str) -> host.Host:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
        if self.args()['identity_file']:
            client.connect(address, key_filename=self.args()['identity_file'])
        else:
            client.connect(address)
        return host.RemoteHost(client)

    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], input: Input) -> Output:
        net = UnreplicatedNet(self._cluster, input)

        # If we're monitoring the code, run garbage collection verbosely.
        java = ['java']
        if input.monitored:
            java += [
                '-verbose:gc',
                '-XX:-PrintGC',
                '-XX:+PrintHeapAtGC',
                '-XX:+PrintGCDetails',
                '-XX:+PrintGCTimeStamps',
                '-XX:+PrintGCDateStamps',
            ]
        # Increase the heap size.
        # TODO(mwhittaker): Right now, not much thought has been put into the
        # heap size. Think more carefully about this. We may want, for example,
        # to increase the size of the young generation.
        java += [f'-Xms{input.jvm_heap_size}', f'-Xmx{input.jvm_heap_size}']

        # Launch server.
        server_proc = bench.popen(
            host=net.placement().server.host,
            label=f'server',
            cmd=java + [
                '-cp',
                os.path.abspath(args['jar']),
                'frankenpaxos.unreplicated.ServerMain',
                '--host',
                net.placement().server.host.ip(),
                '--port',
                str(net.placement().server.port),
                '--log_level',
                input.server_log_level,
                '--state_machine',
                input.state_machine,
                '--prometheus_host',
                net.placement().server.host.ip(),
                '--prometheus_port',
                str(net.placement().server.port +
                    1) if input.monitored else '-1',
                '--options.flushEveryN',
                str(input.server_options.flush_every_n),
            ],
        )
        if input.profiled:
            server_proc = perf_util.JavaPerfProc(bench,
                                                 net.placement().server.host,
                                                 server_proc, f'server')
        bench.log('Servers started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'unreplicated_client': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().clients
                    ],
                    'unreplicated_server': [
                        f'{net.placement().server.host.ip()}:' +
                        f'{net.placement().server.port+1}'
                    ],
                })
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.placement().clients[0].host,
                label='prometheus',
                cmd=[
                    'prometheus',
                    f'--config.file={bench.abspath("prometheus.yml")}',
                    f'--storage.tsdb.path={bench.abspath("prometheus_data")}',
                ],
            )
            bench.log('Prometheus started.')

        # Lag clients.
        time.sleep(input.client_lag.total_seconds())
        bench.log('Client lag ended.')

        # Launch clients.
        workload_filename = bench.abspath('workload.pbtxt')
        bench.write_string(
            workload_filename,
            proto_util.message_to_pbtext(input.workload.to_proto()))

        client_procs: List[proc.Proc] = []
        for (i, client) in enumerate(net.placement().clients):
            p = bench.popen(
                host=client.host,
                label=f'client_{i}',
                # TODO(mwhittaker): For now, we don't run clients with large
                # heaps and verbose garbage collection because they are all
                # colocated on one machine.
                cmd=[
                    'java',
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.unreplicated.ClientMain',
                    '--host',
                    client.host.ip(),
                    '--port',
                    str(client.port),
                    '--server_host',
                    net.placement().server.host.ip(),
                    '--server_port',
                    str(net.placement().server.port),
                    '--log_level',
                    input.client_log_level,
                    '--prometheus_host',
                    client.host.ip(),
                    '--prometheus_port',
                    str(client.port + 1) if input.monitored else '-1',
                    '--measurement_group_size',
                    f'{input.measurement_group_size}',
                    '--warmup_duration',
                    f'{input.warmup_duration.total_seconds()}s',
                    '--warmup_timeout',
                    f'{input.warmup_timeout.total_seconds()}s',
                    '--warmup_sleep',
                    f'{input.warmup_sleep.total_seconds()}s',
                    '--num_warmup_clients',
                    f'{input.num_warmup_clients_per_proc}',
                    '--duration',
                    f'{input.duration.total_seconds()}s',
                    '--timeout',
                    f'{input.timeout.total_seconds()}s',
                    '--num_clients',
                    f'{input.num_clients_per_proc}',
                    '--workload',
                    f'{workload_filename}',
                    '--output_file_prefix',
                    bench.abspath(f'client_{i}'),
                ])
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, client.host, p, f'client_{i}')
            client_procs.append(p)
        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for clients to finish and then terminate leaders and acceptors.
        for p in client_procs:
            p.wait()
        server_proc.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [
            bench.abspath(f'client_{i}_data.csv')
            for i in range(input.num_client_procs)
        ]

        return benchmark.parse_labeled_recorder_data(
            bench,
            client_csvs,
            drop_prefix=datetime.timedelta(seconds=0),
            save_data=False)['write']


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
