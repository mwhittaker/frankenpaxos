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
import itertools
import json
import os
import pandas as pd
import paramiko
import subprocess
import time
import tqdm
import yaml


# Input/Output #################################################################
class ClientOptions(NamedTuple):
    repropose_period: datetime.timedelta = datetime.timedelta(milliseconds=100)


class ProposerOptions(NamedTuple):
    thrifty_system: str = 'NotThrifty'
    resend_phase1as_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_phase2as_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)


class LeaderOptions(NamedTuple):
    thrifty_system: str = 'NotThrifty'
    resend_dependency_requests_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)


class DepServiceNodeOptions(NamedTuple):
    top_k_dependencies: int = -1
    unsafe_return_no_dependencies: bool = False


class AcceptorOptions(NamedTuple):
    pass


class ReplicaOptions(NamedTuple):
    recover_vertex_timer_min_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    recover_vertex_timer_max_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    unsafe_skip_graph_execution: bool = False
    execute_graph_batch_size: int = 1
    execute_graph_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    num_blockers: int = -1


class ZigzagOptions(NamedTuple):
    vertices_grow_size: int = 5000
    garbage_collect_every_n_commands: int = 1000


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    num_leaders: int
    jvm_heap_size: str

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

    # Leader options. ##########################################################
    leader_options: LeaderOptions
    leader_log_level: str

    # Proposer options. ########################################################
    proposer_options: ProposerOptions
    proposer_log_level: str

    # Dep service node options. ################################################
    dep_service_node_options: DepServiceNodeOptions
    dep_service_node_log_level: str

    # Acceptor options. ########################################################
    acceptor_options: AcceptorOptions
    acceptor_log_level: str

    # Replica options. ########################################################
    replica_options: ReplicaOptions
    replica_zigzag_options: ZigzagOptions
    replica_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str


Output = benchmark.RecorderOutput


# Networks #####################################################################
class SimpleBPaxosNet:
    def __init__(self,
                 cluster_file: str,
                 key_filename: Optional[str],
                 input: Input) -> None:
        self._key_filename = key_filename
        # It's important that we initialize the cluster after we set
        # _key_filename since _connect reads _key_filename.
        self._cluster = (cluster.Cluster
                                .from_json_file(cluster_file, self._connect)
                                .f(input.f))
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
        leaders: List[host.Endpoint]
        proposers: List[host.Endpoint]
        dep_service_nodes: List[host.Endpoint]
        acceptors: List[host.Endpoint]
        replicas: List[host.Endpoint]

    def f(self) -> int:
        return self._input.f

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)
        def portify(hosts: List[host.Host]) -> List[host.Endpoint]:
            return [host.Endpoint(h, next(ports)) for h in hosts]

        def cycle_take_n(n: int, hosts: List[host.Host]) -> List[host.Host]:
            return list(itertools.islice(itertools.cycle(hosts), n))

        # TODO(mwhittaker): Pass in the number of every node used.
        n = 2 * self._input.f + 1
        return self.Placement(
            clients = portify(cycle_take_n(
                self._input.num_client_procs, self._cluster['clients'])),
            leaders = portify(cycle_take_n(
                self._input.num_leaders, self._cluster['leaders'])),
            proposers = portify(cycle_take_n(
                self._input.num_leaders, self._cluster['proposers'])),
            dep_service_nodes = portify(cycle_take_n(
                n, self._cluster['dep_service_nodes'])),
            acceptors = portify(cycle_take_n(
                n, self._cluster['acceptors'])),
            replicas = portify(cycle_take_n(
                self._input.f + 1, self._cluster['replicas'])),
        )

    def config(self) -> proto_util.Message:
        return {
            'f': self.f(),
            'leaderAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().leaders
            ],
            'proposerAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().proposers
            ],
            'depServiceNodeAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().dep_service_nodes
            ],
            'acceptorAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().acceptors
            ],
            'replicaAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().replicas
            ],
        }


# Suite ########################################################################
class SimpleBPaxosSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        net = SimpleBPaxosNet(args['cluster'], args['identity_file'], input)
        return self._run_benchmark(bench, args, input, net)

    def _run_benchmark(self,
                       bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any],
                       input: Input,
                       net: SimpleBPaxosNet) -> Output:
        # Write config file.
        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
        bench.log('Config file config.pbtxt written.')

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
        # TODO(mwhittaker): Toggle heap size with a flag.
        java += [f'-Xms{input.jvm_heap_size}', f'-Xmx{input.jvm_heap_size}']

        # Launch dep service nodes.
        dep_service_node_procs: List[proc.Proc] = []
        for (i, dep) in enumerate(net.placement().dep_service_nodes):
            p = bench.popen(
                host=dep.host,
                label=f'dep_service_node_{i}',
                cmd = java + [
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.DepServiceNodeMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.dep_service_node_log_level,
                    '--state_machine', input.state_machine,
                    '--prometheus_host', dep.host.ip(),
                    '--prometheus_port',
                        str(dep.port + 1) if input.monitored else '-1',
                    '--options.topKDependencies',
                        str(input.dep_service_node_options.top_k_dependencies),
                    '--options.unsafeReturnNoDependencies',
                        str(input.dep_service_node_options
                                 .unsafe_return_no_dependencies),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, dep.host, p,
                                           f'dep_service_node_{i}')
            dep_service_node_procs.append(p)
        bench.log('DepServiceNodes started.')

        # Launch acceptors.
        acceptor_procs: List[proc.Proc] = []
        for (i, acceptor) in enumerate(net.placement().acceptors):
            p = bench.popen(
                host=acceptor.host,
                label=f'acceptor_{i}',
                cmd = java + [
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.AcceptorMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.acceptor_log_level,
                    '--prometheus_host', acceptor.host.ip(),
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
                cmd = java + [
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.ReplicaMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.replica_log_level,
                    '--state_machine', input.state_machine,
                    '--prometheus_host', replica.host.ip(),
                    '--prometheus_port',
                        str(replica.port + 1) if input.monitored else '-1',
                    '--options.recoverVertexTimerMinPeriod',
                        '{}s'.format(input.replica_options
                                     .recover_vertex_timer_min_period
                                     .total_seconds()),
                    '--options.recoverVertexTimerMaxPeriod',
                        '{}s'.format(input.replica_options
                                     .recover_vertex_timer_max_period
                                     .total_seconds()),
                    '--options.unsafeSkipGraphExecution',
                        "true"
                        if input.replica_options.unsafe_skip_graph_execution
                        else "false",
                    '--options.executeGraphBatchSize',
                        str(input.replica_options.execute_graph_batch_size),
                    '--options.executeGraphTimerPeriod',
                        '{}s'.format(input.replica_options
                                     .execute_graph_timer_period
                                     .total_seconds()),
                    '--options.numBlockers',
                        str(input.replica_options.num_blockers),
                    '--zigzag.verticesGrowSize',
                        str(input.replica_zigzag_options.vertices_grow_size),
                    '--zigzag.garbageCollectEveryNCommands',
                        str(input.replica_zigzag_options
                                 .garbage_collect_every_n_commands),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, replica.host, p,
                                           f'replica_{i}')
            replica_procs.append(p)
        bench.log('Replicas started.')

        # Launch proposers.
        proposer_procs: List[proc.Proc] = []
        for (i, proposer) in enumerate(net.placement().proposers):
            p = bench.popen(
                host=proposer.host,
                label=f'proposer_{i}',
                cmd = java + [
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.ProposerMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.proposer_log_level,
                    '--prometheus_host', proposer.host.ip(),
                    '--prometheus_port',
                        str(proposer.port + 1) if input.monitored else '-1',
                    '--options.thriftySystem',
                        input.proposer_options.thrifty_system,
                    '--options.resendPhase1asTimerPeriod',
                        '{}s'.format(input.proposer_options
                                     .resend_phase1as_timer_period
                                     .total_seconds()),
                    '--options.resendPhase2asTimerPeriod',
                        '{}s'.format(input.proposer_options
                                     .resend_phase2as_timer_period
                                     .total_seconds()),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, proposer.host, p,
                                           f'proposer_{i}')
            proposer_procs.append(p)
        bench.log('Proposers started.')

        # Launch leaders.
        leader_procs: List[proc.Proc] = []
        for (i, leader) in enumerate(net.placement().leaders):
            p = bench.popen(
                host=leader.host,
                label=f'leader_{i}',
                cmd = java + [
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.LeaderMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.leader_log_level,
                    '--prometheus_host', leader.host.ip(),
                    '--prometheus_port',
                        str(leader.port + 1) if input.monitored else '-1',
                    '--options.thriftySystem',
                        input.leader_options.thrifty_system,
                    '--options.resendDependencyRequestsTimerPeriod',
                        '{}s'.format(input.leader_options
                                     .resend_dependency_requests_timer_period
                                     .total_seconds()),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, leader.host, p,
                                           f'leader_{i}')
            leader_procs.append(p)
        bench.log('Leaders started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000),
                {
                  'bpaxos_leader': [f'{e.host.ip()}:{e.port+1}'
                                    for e in net.placement().leaders],
                  'bpaxos_proposer': [f'{e.host.ip()}:{e.port+1}'
                                      for e in net.placement().proposers],
                  'bpaxos_acceptor': [f'{e.host.ip()}:{e.port+1}'
                                      for e in net.placement().acceptors],
                  'bpaxos_client': [f'{e.host.ip()}:{e.port+1}'
                                    for e in net.placement().clients],
                  'bpaxos_dep_service_node': [f'{e.host.ip()}:{e.port+1}'
                                    for e in net.placement().dep_service_nodes],
                  'bpaxos_replica': [f'{e.host.ip()}:{e.port+1}'
                                     for e in net.placement().replicas],
                }
            )
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.placement().clients[0].host,
                label='prometheus',
                cmd = [
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
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.BenchmarkClientMain',
                    '--host', client.host.ip(),
                    '--port', str(client.port),
                    '--config', config_filename,
                    '--log_level', input.client_log_level,
                    '--prometheus_host', client.host.ip(),
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
                    '--duration', f'{input.duration.total_seconds()}s',
                    '--timeout', f'{input.timeout.total_seconds()}s',
                    '--num_clients', f'{input.num_clients_per_proc}',
                    '--workload', f'{workload_filename}',
                    '--output_file_prefix', bench.abspath(f'client_{i}'),
                    '--options.reproposePeriod',
                        '{}s'.format(input.client_options
                                     .repropose_period
                                     .total_seconds()),
                ]
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, client.host, p, f'client_{i}')
            client_procs.append(p)
        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for clients to finish and then terminate leaders and acceptors.
        for p in client_procs:
            p.wait()
        for p in (leader_procs + proposer_procs + acceptor_procs +
                  dep_service_node_procs + replica_procs):
            p.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        return benchmark.parse_recorder_data(bench, client_csvs,
                drop_prefix=datetime.timedelta(seconds=0))


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
