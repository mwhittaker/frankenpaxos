from .. import benchmark
from .. import cluster
from .. import host
from .. import parser_util
from .. import prometheus
from .. import proto_util
from .. import util
from .. import workload
from ..workload import Workload
from typing import Any, Callable, Collection, Dict, List, NamedTuple, Optional
import argparse
import csv
import datetime
import itertools
import json
import os
import paramiko
import time
import yaml


# Input/Output #################################################################
class ReplicaOptions(NamedTuple):
    thrifty_system: str = 'NotThrifty'
    resend_pre_accepts_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    default_to_slow_path_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_accepts_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_prepares_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    recover_instance_timer_min_period: datetime.timedelta = \
        datetime.timedelta(milliseconds=500)
    recover_instance_timer_max_period: datetime.timedelta = \
        datetime.timedelta(milliseconds=1500)
    unsafe_skip_graph_execution: bool = False
    execute_graph_batch_size: int = 1
    execute_graph_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    num_blockers: int = 1
    top_k_dependencies: int = 1
    unsafe_return_no_dependencies: bool = False

class ZigzagOptions(NamedTuple):
    vertices_grow_size: int = 5000
    garbage_collect_every_n_commands: int = 1000


class ClientOptions(NamedTuple):
    repropose_period: datetime.timedelta = datetime.timedelta(milliseconds=100)


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int

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

    # Replica options. #########################################################
    replica_options: ReplicaOptions
    replica_zigzag_options: ZigzagOptions
    replica_log_level: str

    # Client parameters. #######################################################
    client_options: ClientOptions
    client_log_level: str


Output = benchmark.RecorderOutput


# Network ######################################################################
class EPaxosNet:
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
        replicas: List[host.Endpoint]

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)
        def portify(hosts: List[host.Host]) -> List[host.Endpoint]:
            return [host.Endpoint(h, next(ports)) for h in hosts]

        def cycle_take_n(n: int, hosts: List[host.Host]) -> List[host.Host]:
            return list(itertools.islice(itertools.cycle(hosts), n))

        return self.Placement(
            clients = portify(cycle_take_n(
                self._input.num_client_procs, self._cluster['clients'])),
            replicas = portify(cycle_take_n(
                2*self._input.f + 1, self._cluster['replicas'])),
        )

    def config(self) -> proto_util.Message:
        return {
            'f': self._input.f,
            'replicaAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().replicas
            ],
        }


# Suite ########################################################################
class EPaxosSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        net = EPaxosNet(args['cluster'], args['identity_file'], input)
        return self._run_benchmark(bench, args, input, net)

    def _run_benchmark(self,
                       bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any],
                       input: Input,
                       net: EPaxosNet) -> Output:
        # Write config file.
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(net.config()))
        bench.log('Config file config.pbtxt written.')

        # Launch replicas.
        replica_procs = []
        for (i, replica) in enumerate(net.placement().replicas):
            proc = bench.popen(
                host=replica.host,
                label=f'replica_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.epaxos.ReplicaMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.replica_log_level,
                    '--state_machine', input.state_machine,
                    '--prometheus_host', replica.host.ip(),
                    '--prometheus_port',
                        str(replica.port + 1) if input.monitored else '-1',
                    '--options.thriftySystem',
                        input.replica_options.thrifty_system,
                    '--options.resendPreAcceptsTimerPeriod', '{}ms'.format(
                        input.replica_options
                             .resend_pre_accepts_timer_period
                             .total_seconds() * 1000),
                    '--options.defaultToSlowPathTimerPeriod', '{}ms'.format(
                        input.replica_options
                             .default_to_slow_path_timer_period
                             .total_seconds() * 1000),
                    '--options.resendAcceptsTimerPeriod', '{}ms'.format(
                        input.replica_options
                             .resend_accepts_timer_period
                             .total_seconds() * 1000),
                    '--options.resendPreparesTimerPeriod', '{}ms'.format(
                        input.replica_options
                             .resend_prepares_timer_period
                             .total_seconds() * 1000),
                    '--options.recoverInstanceTimerMinPeriod', '{}ms'.format(
                        input.replica_options
                             .recover_instance_timer_min_period
                             .total_seconds() * 1000),
                    '--options.recoverInstanceTimerMaxPeriod', '{}ms'.format(
                        input.replica_options
                             .recover_instance_timer_max_period
                             .total_seconds() * 1000),
                    '--options.unsafeSkipGraphExecution',
                        "true"
                        if input.replica_options.unsafe_skip_graph_execution
                        else "false",
                    '--options.executeGraphBatchSize',
                        str(input.replica_options.execute_graph_batch_size),
                    '--options.executeGraphTimerPeriod', '{}s'.format(
                        input.replica_options
                             .execute_graph_timer_period
                             .total_seconds()),
                    '--options.numBlockers',
                        str(input.replica_options.num_blockers),
                    '--options.topKDependencies',
                        str(input.replica_options.top_k_dependencies),
                    '--options.unsafeReturnNoDependencies',
                        "true"
                        if input.replica_options.unsafe_return_no_dependencies
                        else "false",
                    '--zigzag.verticesGrowSize',
                        str(input.replica_zigzag_options.vertices_grow_size),
                    '--zigzag.garbageCollectEveryNCommands',
                        str(input.replica_zigzag_options
                                 .garbage_collect_every_n_commands),
                ],
            )
            replica_procs.append(proc)
        bench.log('Replicas started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000),
                {
                  'epaxos_replica': [f'{e.host.ip()}:{e.port + 1}'
                                     for e in net.placement().replicas],
                  'epaxos_client': [f'{e.host.ip()}:{e.port + 1}'
                                    for e in net.placement().clients],
                }
            )
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.placement().replicas[0].host,
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

        client_procs = []
        for (i, client) in enumerate(net.placement().clients):
            proc = bench.popen(
                host=client.host,
                label=f'client_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.epaxos.BenchmarkClientMain',
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
            client_procs.append(proc)
        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for clients to finish and then terminate leaders and acceptors.
        for proc in client_procs:
            proc.wait()
        for proc in replica_procs:
            proc.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        return benchmark.parse_recorder_data(bench, client_csvs,
                drop_prefix=input.warmup_duration + input.warmup_sleep)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
