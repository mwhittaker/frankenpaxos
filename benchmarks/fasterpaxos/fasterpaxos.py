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
    resend_client_request_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)


class HeartbeatOptions(NamedTuple):
    fail_period: datetime.timedelta = datetime.timedelta(seconds=1)
    success_period: datetime.timedelta = datetime.timedelta(seconds=2)
    num_retries: int = 3
    network_delay_alpha: float = 0.9


class ServerOptions(NamedTuple):
    ack_noops_with_commands: bool = False
    log_grow_size: int = 1000
    resend_phase1as_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_phase2a_anys_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    use_f1_optimization: bool = False
    recover_log_entry_min_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    recover_log_entry_max_period: datetime.timedelta = \
        datetime.timedelta(seconds=2)
    leader_change_entry_min_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    leader_change_entry_max_period: datetime.timedelta = \
        datetime.timedelta(seconds=2)
    unsafe_dont_recover: bool = False
    heartbeat_options: HeartbeatOptions = HeartbeatOptions()


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    num_servers: int
    client_jvm_heap_size: str
    server_jvm_heap_size: str

    # Benchmark parameters. ####################################################
    measurement_group_size: int
    warmup_duration: datetime.timedelta
    warmup_timeout: datetime.timedelta
    warmup_sleep: datetime.timedelta
    duration: datetime.timedelta
    timeout: datetime.timedelta
    client_lag: datetime.timedelta
    state_machine: str
    workload: workload.Workload
    profiled: bool
    monitored: bool
    prometheus_scrape_interval: datetime.timedelta

    # Server options. ##########################################################
    server_options: ServerOptions
    server_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str


class FasterPaxosOutput(NamedTuple):
    output: benchmark.RecorderOutput


Output = FasterPaxosOutput


# Networks #####################################################################
class FasterPaxosNet:
    def __init__(self, cluster: cluster.Cluster, input: Input) -> None:
        self._cluster = cluster.f(input.f)
        self._input = input

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        servers: List[host.Endpoint]
        heartbeats: List[host.Endpoint]

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)

        def portify(hosts: List[host.Host]) -> List[host.Endpoint]:
            return [host.Endpoint(h, next(ports)) for h in hosts]

        def cycle_take_n(n: int, hosts: List[host.Host]) -> List[host.Host]:
            return list(itertools.islice(itertools.cycle(hosts), n))

        return self.Placement(
            clients=portify(
                cycle_take_n(self._input.num_client_procs,
                             self._cluster['clients'])),
            servers=portify(
                cycle_take_n(self._input.num_servers,
                             self._cluster['servers'])),
            heartbeats=portify(
                cycle_take_n(self._input.num_servers,
                             self._cluster['servers'])),
        )

    def config(self) -> proto_util.Message:
        return {
            'f': self._input.f,
            'server_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().servers],
            'heartbeat_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().heartbeats],
        }


# Suite ########################################################################
class FasterPaxosSuite(benchmark.Suite[Input, Output]):
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

    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
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

        # Write config file.
        net = FasterPaxosNet(self._cluster, input)
        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
        bench.log('Config file config.pbtxt written.')

        # Launch servers.
        server_procs: List[proc.Proc] = []
        for (i, server) in enumerate(net.placement().servers):
            p = bench.popen(
                host=server.host,
                label=f'server_{i}',
                cmd=java(input.server_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.fasterpaxos.ServerMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.server_log_level,
                    '--state_machine',
                    input.state_machine,
                    '--prometheus_host',
                    server.host.ip(),
                    '--prometheus_port',
                    str(server.port + 1) if input.monitored else '-1',
                    '--options.ackNoopsWithCommands',
                    str(input.server_options.ack_noops_with_commands),
                    '--options.logGrowSize',
                    str(input.server_options.log_grow_size),
                    '--options.resendPhase1asPeriod',
                    '{}s'.format(input.server_options
                                      .resend_phase1as_period
                                      .total_seconds()),
                    '--options.resendPhase2aAnysPeriod',
                    '{}s'.format(input.server_options
                                      .resend_phase2a_anys_period
                                      .total_seconds()),
                    '--options.useF1Optimization',
                    str(input.server_options.use_f1_optimization),
                    '--options.recoverLogEntryMinPeriod',
                    '{}s'.format(input.server_options
                                      .recover_log_entry_min_period
                                      .total_seconds()),
                    '--options.recoverLogEntryMaxPeriod',
                    '{}s'.format(input.server_options
                                      .recover_log_entry_max_period
                                      .total_seconds()),
                    '--options.leaderChangeEntryMinPeriod',
                    '{}s'.format(input.server_options
                                      .leader_change_entry_min_period
                                      .total_seconds()),
                    '--options.leaderChangeEntryMaxPeriod',
                    '{}s'.format(input.server_options
                                      .leader_change_entry_max_period
                                      .total_seconds()),
                    '--options.unsafeDontRecover',
                    str(input.server_options.unsafe_dont_recover),
                    '--options.heartbeat.failPeriod',
                    '{}s'.format(input.server_options
                                      .heartbeat_options
                                      .fail_period
                                      .total_seconds()),
                    '--options.heartbeat.successPeriod',
                    '{}s'.format(input.server_options
                                      .heartbeat_options
                                      .success_period
                                      .total_seconds()),
                    '--options.heartbeat.numRetries',
                    str(input.server_options.heartbeat_options.num_retries),
                    '--options.heartbeat.networkDelayAlpha',
                    str(input.server_options
                             .heartbeat_options
                             .network_delay_alpha),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, server.host, p,
                                           f'server_{i}')
            server_procs.append(p)
        bench.log('Servers started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'fasterpaxos_client': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().clients
                    ],
                    'fasterpaxos_server': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().servers
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
                cmd=java(input.client_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.fasterpaxos.ClientMain',
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
                    '--output_file_prefix',
                    bench.abspath(f'client_{i}'),
                    '--workload',
                    f'{workload_filename}',
                    '--options.resendClientRequestPeriod',
                    '{}s'.format(input.client_options.
                                 resend_client_request_period.total_seconds()),
                ])
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, client.host, p, f'client_{i}')
            client_procs.append(p)
        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for clients to finish and then terminate leaders and acceptors.
        for p in client_procs:
            p.wait()
        for p in server_procs:
            p.kill()
        if input.monitored:
            prometheus_server.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [
            bench.abspath(f'client_{i}_data.csv')
            for i in range(input.num_client_procs)
        ]

        labeled_data = benchmark.parse_labeled_recorder_data(
            bench,
            client_csvs,
            drop_prefix=datetime.timedelta(seconds=0),
            save_data=False)
        output = labeled_data['write']
        return FasterPaxosOutput(output = output)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
