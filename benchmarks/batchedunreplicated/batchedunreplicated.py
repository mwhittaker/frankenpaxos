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


class BatcherOptions(NamedTuple):
    batch_size: int = 1


class ServerOptions(NamedTuple):
    flush_every_n: int = 1


class ProxyServerOptions(NamedTuple):
    flush_every_n: int = 1


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    num_batchers: int
    num_proxy_servers: int
    client_jvm_heap_size: str
    batcher_jvm_heap_size: str
    server_jvm_heap_size: str
    proxy_server_jvm_heap_size: str

    # Benchmark parameters. ####################################################
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

    # Batcher options. #########################################################
    batcher_options: BatcherOptions
    batcher_log_level: str

    # Server options. ##########################################################
    server_options: ServerOptions
    server_log_level: str

    # ProxyServer options. #####################################################
    proxy_server_options: ProxyServerOptions
    proxy_server_log_level: str


Output = benchmark.RecorderOutput


# Networks #####################################################################
class BatchedUnreplicatedNet:
    def __init__(self, cluster_file: str, key_filename: Optional[str],
                 input: Input) -> None:
        self._key_filename = key_filename
        # It's important that we initialize the cluster after we set
        # _key_filename since _connect reads _key_filename.
        self._cluster = (cluster.Cluster.from_json_file(cluster_file,
                                                        self._connect).f(1))
        self._input = input

    def _connect(self, address: str) -> host.Host:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
        if self._key_filename:
            client.connect(address, key_filename=self._key_filename)
        else:
            client.connect(address)
        return host.RemoteHost(client)

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        batchers: List[host.Endpoint]
        server: host.Endpoint
        proxy_servers: List[host.Endpoint]

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
            batchers=portify(
                cycle_take_n(self._input.num_batchers,
                self._cluster['batchers']
                )
                ),
            server=portify_one(self._cluster['server'][0]),
            proxy_servers=portify(
                cycle_take_n(self._input.num_proxy_servers,
                self._cluster['proxy_servers']
                )
                ),
        )

    def config(self) -> proto_util.Message:
        return {
            'batcher_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().batchers],
            'server_address': {
                'host': self.placement().server.host.ip(),
                'port': self.placement().server.port
            },
            'proxy_server_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().proxy_servers],
        }


# Suite ########################################################################
class BatchedUnreplicatedSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], input: Input) -> Output:
        net = BatchedUnreplicatedNet(args['cluster'], args['identity_file'], input)
        return self._run_benchmark(bench, args, input, net)

    def _run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any], input: Input,
                       net: BatchedUnreplicatedNet) -> Output:
        # Write config file.
        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
        bench.log('Config file config.pbtxt written.')

        # If we're monitoring the code, run garbage collection verbosely.
        def java(heap_size: str) -> List[str]:
            cmd = ['java', f'-Xms{heap_size}', f'-Xmx{heap_size}']
            if input.monitored:
                cmd += [
                    '-verbose:gc',
                    '-XX:-PrintGC',
                    '-XX:+PrintHeapAtGC',
                    '-XX:+PrintGCDetails',
                    '-XX:+PrintGCTimeStamps',
                    '-XX:+PrintGCDateStamps',
                ]
            return cmd

        # Launch batchers.
        batcher_procs: List[proc.Proc] = []
        for (i, batcher) in enumerate(net.placement().batchers):
            p = bench.popen(
                host=batcher.host,
                label=f'batcher_{i}',
                cmd=java(input.batcher_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.batchedunreplicated.BatcherMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.batcher_log_level,
                    '--prometheus_host',
                    batcher.host.ip(),
                    '--prometheus_port',
                    str(batcher.port + 1) if input.monitored else '-1',
                    '--options.batchSize',
                    str(input.batcher_options.batch_size),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, batcher.host, p,
                                           f'batcher_{i}')
            batcher_procs.append(p)
        bench.log('Batchers started.')

        # Launch proxy_servers.
        proxy_server_procs: List[proc.Proc] = []
        for (i, proxy_server) in enumerate(net.placement().proxy_servers):
            p = bench.popen(
                host=proxy_server.host,
                label=f'proxy_server_{i}',
                cmd=java(input.proxy_server_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.batchedunreplicated.ProxyServerMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.proxy_server_log_level,
                    '--prometheus_host',
                    proxy_server.host.ip(),
                    '--prometheus_port',
                    str(proxy_server.port + 1) if input.monitored else '-1',
                    '--options.flushEveryN',
                    str(input.proxy_server_options.flush_every_n),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, proxy_server.host, p,
                                           f'proxy_server_{i}')
            proxy_server_procs.append(p)
        bench.log('ProxyServers started.')

        # Launch server.
        server_proc = bench.popen(
            host=net.placement().server.host,
            label=f'server',
            cmd=java(input.server_jvm_heap_size) + [
                '-cp',
                os.path.abspath(args['jar']),
                'frankenpaxos.batchedunreplicated.ServerMain',
                '--config',
                config_filename,
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
                    'batchedunreplicated_client': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().clients
                    ],
                    'batchedunreplicated_batcher': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().batchers
                    ],
                    'batchedunreplicated_server': [
                        f'{net.placement().server.host.ip()}:' +
                        f'{net.placement().server.port+1}'
                    ],
                    'batchedunreplicated_proxy_server': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().proxy_servers
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
                cmd= java(input.client_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.batchedunreplicated.ClientMain',
                    '--host',
                    client.host.ip(),
                    '--port',
                    str(client.port),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.client_log_level,
                    '--prometheus_host',
                    client.host.ip(),
                    '--prometheus_port',
                    str(client.port + 1) if input.monitored else '-1',
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
        for p in batcher_procs + [server_proc] + proxy_server_procs:
            p.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [
            bench.abspath(f'client_{i}_data.csv')
            for i in range(input.num_client_procs)
        ]
        return benchmark.parse_recorder_data(
            bench, client_csvs, drop_prefix=datetime.timedelta(seconds=0))


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
