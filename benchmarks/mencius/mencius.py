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
class DistributionScheme(enum.Enum):
    HASH = 'HASH'
    COLOCATED = 'COLOCATED'


class ClientOptions(NamedTuple):
    resend_client_request_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)


class BatcherOptions(NamedTuple):
    batch_size: int = 1


class ElectionOptions(NamedTuple):
    ping_period: datetime.timedelta = datetime.timedelta(seconds=1)
    no_ping_timeout_min: datetime.timedelta = datetime.timedelta(seconds=5)
    no_ping_timeout_max: datetime.timedelta = datetime.timedelta(seconds=10)


class LeaderOptions(NamedTuple):
    send_high_watermark_every_n: int = 1000
    send_noop_range_if_lagging_by: int = 5000
    resend_phase1as_period: datetime.timedelta = datetime.timedelta(seconds=1)
    flush_phase2as_every_n: int = 1
    election_options: ElectionOptions = ElectionOptions()


class ProxyLeaderOptions(NamedTuple):
    flush_phase2as_every_n: int = 1


class AcceptorOptions(NamedTuple):
    pass


class ReplicaOptions(NamedTuple):
    log_grow_size: int = 1000
    unsafe_dont_use_client_table: bool = False
    send_chosen_watermark_every_n_entries: int = 100
    recover_log_entry_min_period: datetime.timedelta = \
        datetime.timedelta(seconds=10)
    recover_log_entry_max_period: datetime.timedelta = \
        datetime.timedelta(seconds=20)
    unsafe_dont_recover: bool = False


class ProxyReplicaOptions(NamedTuple):
    flush_every_n: int = 1


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    num_batchers: int
    num_leader_groups: int
    num_leaders_per_group: int
    num_proxy_leaders: int
    num_acceptor_groups_per_leader_group: int
    num_acceptors_per_group: int
    num_replicas: int
    num_proxy_replicas: int
    distribution_scheme: DistributionScheme
    client_jvm_heap_size: str
    batcher_jvm_heap_size: str
    leader_jvm_heap_size: str
    proxy_leader_jvm_heap_size: str
    acceptor_jvm_heap_size: str
    replica_jvm_heap_size: str
    proxy_replica_jvm_heap_size: str

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

    # Batcher options. #########################################################
    batcher_options: BatcherOptions
    batcher_log_level: str

    # Leader options. ##########################################################
    leader_options: LeaderOptions
    leader_log_level: str

    # ProxyLeader options. #####################################################
    proxy_leader_options: ProxyLeaderOptions
    proxy_leader_log_level: str

    # Acceptor options. ########################################################
    acceptor_options: AcceptorOptions
    acceptor_log_level: str

    # Replica options. #########################################################
    replica_options: ReplicaOptions
    replica_log_level: str

    # ProxyReplica options. ####################################################
    proxy_replica_options: ProxyReplicaOptions
    proxy_replica_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str


Output = benchmark.RecorderOutput


# Networks #####################################################################
class MenciusNet:
    def __init__(self, cluster_file: str, key_filename: Optional[str],
                 input: Input) -> None:
        self._key_filename = key_filename
        # It's important that we initialize the cluster after we set
        # _key_filename since _connect reads _key_filename.
        self._cluster = (cluster.Cluster.from_json_file(
            cluster_file, self._connect).f(input.f))
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
        leaders: List[List[host.Endpoint]]
        leader_elections: List[List[host.Endpoint]]
        proxy_leaders: List[host.Endpoint]
        acceptors: List[List[List[host.Endpoint]]]
        replicas: List[host.Endpoint]
        proxy_replicas: List[host.Endpoint]

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)

        def portify(hosts: List[host.Host]) -> List[host.Endpoint]:
            return [host.Endpoint(h, next(ports)) for h in hosts]

        def cycle_take_n(n: int, hosts: List[host.Host]) -> List[host.Host]:
            return list(itertools.islice(itertools.cycle(hosts), n))

        def chunks(xs, n):
            # https://stackoverflow.com/a/312464/3187068
            return [xs[i:i + n] for i in range(0, len(xs), n)]

        # Place the leaders.
        if len(self._cluster['leaders']) == self._input.num_leaders_per_group:
            # If the number of leader machines available is equal to the number
            # of leaders per group, then we stagger which machine is the leader
            # of every group. This idea is taken from [1].
            #
            # [1]: scholar.google.com/scholar?cluster=9598150994777714641
            leader_hosts = [
                list(
                    itertools.islice(
                        itertools.cycle(self._cluster['leaders']),
                        i,
                        i + self._input.num_leaders_per_group))
                for i in range(self._input.num_leader_groups)
            ]
        else:
            num_leaders = (self._input.num_leader_groups *
                           self._input.num_leaders_per_group)
            leader_hosts = chunks(
                cycle_take_n(num_leaders, self._cluster['leaders']),
                self._input.num_leaders_per_group)

        # Place the acceptors.
        num_acceptors_per_leader_group = (
            self._input.num_acceptor_groups_per_leader_group *
            self._input.num_acceptors_per_group)
        num_acceptors = (self._input.num_leader_groups *
                         num_acceptors_per_leader_group)
        acceptor_hosts = [
            chunks(leader_group, self._input.num_acceptors_per_group)
            for leader_group in chunks(
                cycle_take_n(num_acceptors, self._cluster['acceptors']),
                num_acceptors_per_leader_group)
        ]

        n = 2 * self._input.f + 1
        return self.Placement(
            clients=portify(
                cycle_take_n(self._input.num_client_procs,
                             self._cluster['clients'])),
            batchers=portify(
                cycle_take_n(self._input.num_batchers,
                             self._cluster['batchers'])),
            leaders=[portify(group) for group in leader_hosts],
            leader_elections=[portify(group) for group in leader_hosts],
            proxy_leaders=portify(
                cycle_take_n(self._input.num_proxy_leaders,
                             self._cluster['proxy_leaders'])),
            acceptors=[[
                portify(acceptor_group) for acceptor_group in leader_group
            ] for leader_group in acceptor_hosts],
            replicas=portify(
                cycle_take_n(self._input.num_replicas,
                             self._cluster['replicas'])),
            proxy_replicas=portify(
                cycle_take_n(self._input.num_proxy_replicas,
                             self._cluster['proxy_replicas'])),
        )

    def config(self) -> proto_util.Message:
        return {
            'f': self._input.f,
            'batcher_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().batchers],
            'leader_address': [{
                'address': [{
                    'host': e.host.ip(),
                    'port': e.port
                } for e in group]
            } for group in self.placement().leaders],
            'leader_election_address': [{
                'address': [{
                    'host': e.host.ip(),
                    'port': e.port
                } for e in group]
            } for group in self.placement().leader_elections],
            'proxy_leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().proxy_leaders],
            'acceptor_address': [{
                'groups': [{
                    'address': [{
                        'host': e.host.ip(),
                        'port': e.port
                    } for e in group]
                } for group in groupgroup]
            } for groupgroup in self.placement().acceptors],
            'replica_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().replicas],
            'proxy_replica_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().proxy_replicas],
            'distribution_scheme': self._input.distribution_scheme,
        }


# Suite ########################################################################
class MenciusSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], input: Input) -> Output:
        net = MenciusNet(args['cluster'], args['identity_file'], input)
        return self._run_benchmark(bench, args, input, net)

    def _run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any], input: Input,
                       net: MenciusNet) -> Output:
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
                    'frankenpaxos.mencius.BatcherMain',
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

        # Launch proxy_leaders.
        proxy_leader_procs: List[proc.Proc] = []
        for (i, proxy_leader) in enumerate(net.placement().proxy_leaders):
            p = bench.popen(
                host=proxy_leader.host,
                label=f'proxy_leader_{i}',
                cmd=java(input.proxy_leader_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.mencius.ProxyLeaderMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.proxy_leader_log_level,
                    '--prometheus_host',
                    proxy_leader.host.ip(),
                    '--prometheus_port',
                    str(proxy_leader.port + 1) if input.monitored else '-1',
                    '--options.flushPhase2asEveryN',
                    str(input.proxy_leader_options.flush_phase2as_every_n),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, proxy_leader.host, p,
                                           f'proxy_leader_{i}')
            proxy_leader_procs.append(p)
        bench.log('ProxyLeaders started.')

        # Launch acceptors.
        acceptor_procs: List[proc.Proc] = []
        for (leader_group_index,
             leader_group) in enumerate(net.placement().acceptors):
            for (acceptor_group_index,
                 acceptor_group) in enumerate(leader_group):
                for (i, acceptor) in enumerate(acceptor_group):
                    label = (f'acceptor_{leader_group_index}_'
                             f'{acceptor_group_index}_{i}')
                    p = bench.popen(
                        host=acceptor.host,
                        label=label,
                        cmd=java(input.acceptor_jvm_heap_size) + [
                            '-cp',
                            os.path.abspath(args['jar']),
                            'frankenpaxos.mencius.AcceptorMain',
                            '--leader_group_index',
                            str(leader_group_index),
                            '--acceptor_group_index',
                            str(acceptor_group_index),
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
                                                   label)
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
                    'frankenpaxos.mencius.ReplicaMain',
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
                    '--options.sendChosenWatermarkEveryNEntries',
                    str(input.replica_options.
                        send_chosen_watermark_every_n_entries),
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

        # Launch proxy_replicas.
        proxy_replica_procs: List[proc.Proc] = []
        for (i, proxy_replica) in enumerate(net.placement().proxy_replicas):
            p = bench.popen(
                host=proxy_replica.host,
                label=f'proxy_replica_{i}',
                cmd=java(input.proxy_replica_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.mencius.ProxyReplicaMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.proxy_replica_log_level,
                    '--prometheus_host',
                    proxy_replica.host.ip(),
                    '--prometheus_port',
                    str(proxy_replica.port + 1) if input.monitored else '-1',
                    '--options.flushEveryN',
                    str(input.proxy_replica_options.flush_every_n),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, proxy_replica.host, p,
                                           f'proxy_replica_{i}')
            proxy_replica_procs.append(p)
        bench.log('ProxyReplicas started.')

        # Launch leaders.
        leader_procs: List[proc.Proc] = []
        for (group_index, group) in enumerate(net.placement().leaders):
            for (i, leader) in enumerate(group):
                p = bench.popen(
                    host=leader.host,
                    label=f'leader_{group_index}_{i}',
                    cmd=java(input.leader_jvm_heap_size) + [
                        '-cp',
                        os.path.abspath(args['jar']),
                        'frankenpaxos.mencius.LeaderMain',
                        '--group_index',
                        str(group_index),
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
                        '--options.sendHighWatermarkEveryN',
                        str(input.leader_options.send_high_watermark_every_n),
                        '--options.sendNoopRangeIfLaggingBy',
                        str(input.leader_options.send_noop_range_if_lagging_by),
                        '--options.resendPhase1asPeriod',
                        '{}s'.format(input.leader_options.
                                     resend_phase1as_period.total_seconds()),
                        '--options.flushPhase2asEveryN',
                        str(input.leader_options.flush_phase2as_every_n),
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
                    p = perf_util.JavaPerfProc(bench, leader.host, p,
                                               f'leader_{group_index}_{i}')
                leader_procs.append(p)
        bench.log('Leaders started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'mencius_client': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().clients
                    ],
                    'mencius_batcher': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().batchers
                    ],
                    'mencius_leader': [
                        f'{e.host.ip()}:{e.port+1}'
                        for group in net.placement().leaders
                        for e in group
                    ],
                    'mencius_proxy_leader': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().proxy_leaders
                    ],
                    'mencius_acceptor': [
                        f'{e.host.ip()}:{e.port+1}'
                        for leader_group in net.placement().acceptors
                        for acceptor_group in leader_group
                        for e in acceptor_group
                    ],
                    'mencius_replica': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().replicas
                    ],
                    'mencius_proxy_replica': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().proxy_replicas
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
                    'frankenpaxos.mencius.ClientMain',
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
        for p in (batcher_procs + leader_procs + proxy_leader_procs +
                  acceptor_procs + replica_procs + proxy_replica_procs):
            p.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [
            bench.abspath(f'client_{i}_data.csv')
            for i in range(input.num_client_procs)
        ]
        return benchmark.parse_recorder_data(
            bench, client_csvs, drop_prefix=datetime.timedelta(seconds=0),
            save_data=False)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
