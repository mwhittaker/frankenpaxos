from . import driver_workload
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


class ElectionOptions(NamedTuple):
    ping_period: datetime.timedelta = datetime.timedelta(seconds=1)
    no_ping_timeout_min: datetime.timedelta = datetime.timedelta(seconds=5)
    no_ping_timeout_max: datetime.timedelta = datetime.timedelta(seconds=10)


class LeaderOptions(NamedTuple):
    log_grow_size: int = 1000
    alpha: int = 1000
    resend_phase1as_period: datetime.timedelta = datetime.timedelta(seconds=1)
    election_options: ElectionOptions = ElectionOptions()


class AcceptorOptions(NamedTuple):
    pass


class ReplicaOptions(NamedTuple):
    log_grow_size: int = 1000
    unsafe_dont_use_client_table: bool = False
    recover_log_entry_min_period: datetime.timedelta = \
        datetime.timedelta(seconds=10)
    recover_log_entry_max_period: datetime.timedelta = \
        datetime.timedelta(seconds=20)
    unsafe_dont_recover: bool = False


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    num_leaders: int
    num_acceptors: int
    num_replicas: int
    client_jvm_heap_size: str
    leader_jvm_heap_size: str
    acceptor_jvm_heap_size: str
    replica_jvm_heap_size: str
    driver_jvm_heap_size: str

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
    driver_workload: driver_workload.DriverWorkload
    profiled: bool
    monitored: bool
    prometheus_scrape_interval: datetime.timedelta

    # Leader options. ##########################################################
    leader_options: LeaderOptions
    leader_log_level: str

    # Acceptor options. ########################################################
    acceptor_options: AcceptorOptions
    acceptor_log_level: str

    # Replica options. #########################################################
    replica_options: ReplicaOptions
    replica_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str

    # Driver options. ##########################################################
    driver_log_level: str


Output = benchmark.RecorderOutput


# Networks #####################################################################
class HorizontalNet:
    def __init__(self, cluster: cluster.Cluster, input: Input) -> None:
        self._cluster = cluster.f(input.f)
        self._input = input

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        leaders: List[host.Endpoint]
        leader_elections: List[host.Endpoint]
        acceptors: List[host.Endpoint]
        replicas: List[host.Endpoint]
        driver: host.Endpoint

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
            leaders=portify(
                cycle_take_n(self._input.num_leaders,
                             self._cluster['leaders'])),
            leader_elections=portify(
                cycle_take_n(self._input.num_leaders,
                             self._cluster['leaders'])),
            acceptors=portify(
                cycle_take_n(self._input.num_acceptors,
                             self._cluster['acceptors'])),
            replicas=portify(
                cycle_take_n(self._input.num_replicas,
                             self._cluster['replicas'])),
            driver=portify(self._cluster['driver'])[0],
        )

    def config(self) -> proto_util.Message:
        return {
            'f': self._input.f,
            'leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().leaders],
            'leader_election_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().leader_elections],
            'acceptor_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().acceptors],
            'replica_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().replicas],
        }


# Suite ########################################################################
class HorizontalSuite(benchmark.Suite[Input, Output]):
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
        net = HorizontalNet(self._cluster, input)
        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
        bench.log('Config file config.pbtxt written.')

        # Launch acceptors.
        acceptor_procs: List[proc.Proc] = []
        for (i, acceptor) in enumerate(net.placement().acceptors):
            p = bench.popen(
                host=acceptor.host,
                label=f'acceptor_{i}',
                cmd=java(input.acceptor_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.horizontal.AcceptorMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.acceptor_log_level,
                    '--prometheus_host',
                    acceptor.host.ip(),
                    '--prometheus_port',
                    str(acceptor.port + 1) if input.monitored else '-1',
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, acceptor.host, p,
                                           f'acceptor_{i}')
            acceptor_procs.append(p)
        bench.log('Acceptors started.')

        # Launch replicas.
        replica_procs: List[proc.Proc] = []
        for (i, replica) in enumerate(net.placement().replicas):
            p = bench.popen(
                host=replica.host,
                label=f'replica_{i}',
                cmd=java(input.replica_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.horizontal.ReplicaMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.replica_log_level,
                    '--state_machine',
                    input.state_machine,
                    '--prometheus_host',
                    replica.host.ip(),
                    '--prometheus_port',
                    str(replica.port + 1) if input.monitored else '-1',
                    '--options.logGrowSize',
                    str(input.replica_options.log_grow_size),
                    '--options.unsafeDontUseClientTable',
                    str(input.replica_options.unsafe_dont_use_client_table),
                    '--options.recoverLogEntryMinPeriod',
                    '{}s'.format(input.replica_options.
                                 recover_log_entry_min_period.total_seconds()),
                    '--options.recoverLogEntryMaxPeriod',
                    '{}s'.format(input.replica_options.
                                 recover_log_entry_max_period.total_seconds()),
                    '--options.unsafeDontRecover',
                    str(input.replica_options.unsafe_dont_recover),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, replica.host, p,
                                           f'replica_{i}')
            replica_procs.append(p)
        bench.log('Replicas started.')

        # Launch leaders.
        leader_procs: List[proc.Proc] = []
        for (i, leader) in enumerate(net.placement().leaders):
            p = bench.popen(
                host=leader.host,
                label=f'leader_{i}',
                cmd=java(input.leader_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.horizontal.LeaderMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.leader_log_level,
                    '--prometheus_host',
                    leader.host.ip(),
                    '--prometheus_port',
                    str(leader.port + 1) if input.monitored else '-1',
                    '--options.logGrowSize',
                    str(input.leader_options.log_grow_size),
                    '--options.alpha',
                    str(input.leader_options.alpha),
                    '--options.resendPhase1asPeriod',
                    '{}s'.format(input.leader_options.resend_phase1as_period.
                                 total_seconds()),
                    '--options.election.pingPeriod',
                    '{}s'.format(input.leader_options.election_options.
                                 ping_period.total_seconds()),
                    '--options.election.noPingTimeoutMin',
                    '{}s'.format(input.leader_options.election_options.
                                 no_ping_timeout_min.total_seconds()),
                    '--options.election.noPingTimeoutMax',
                    '{}s'.format(input.leader_options.election_options.
                                 no_ping_timeout_max.total_seconds()),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, leader.host, p, f'leader_{i}')
            leader_procs.append(p)
        bench.log('Leaders started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'horizontal_client': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().clients
                    ],
                    'horizontal_leader': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().leaders
                    ],
                    'horizontal_acceptor': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().acceptors
                    ],
                    'horizontal_replica': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().replicas
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
                cmd=java(input.client_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.horizontal.ClientMain',
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
                    "--options.resendClientRequestPeriod",
                    '{}s'.format(input.client_options.
                                 resend_client_request_period.total_seconds()),
                ])
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, client.host, p, f'client_{i}')
            client_procs.append(p)
        bench.log(f'Clients started and running for {input.duration}.')

        # Launch driver.
        driver_workload_filename = bench.abspath('driver_workload.pbtxt')
        bench.write_string(
            driver_workload_filename,
            proto_util.message_to_pbtext(input.driver_workload.to_proto()))

        driver_proc: proc.Proc = bench.popen(
            host=net.placement().driver.host,
            label=f'driver',
            cmd=java(input.driver_jvm_heap_size) + [
                '-cp',
                os.path.abspath(args['jar']),
                'frankenpaxos.horizontal.DriverMain',
                '--host',
                net.placement().driver.host.ip(),
                '--port',
                str(net.placement().driver.port),
                '--config',
                config_filename,
                '--log_level',
                input.driver_log_level,
                '--driver_workload',
                f'{driver_workload_filename}',
            ])
        bench.log('Driver started')

        # Wait for clients to finish and then terminate leaders and acceptors.
        for p in client_procs:
            p.wait()
        for p in (leader_procs + acceptor_procs + replica_procs +
                  [driver_proc]):
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
        return labeled_data['write']


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
