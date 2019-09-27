from .. import benchmark
from .. import cluster
from .. import host
from .. import parser_util
from .. import pd_util
from .. import prometheus
from .. import proto_util
from .. import util
from .. import workload
from ..workload import Workload
from typing import Any, Callable, Dict, List, NamedTuple, Optional
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
import yaml


# Input/Output #################################################################
class RoundSystemType(enum.Enum):
    CLASSIC_ROUND_ROBIN = 'CLASSIC_ROUND_ROBIN'
    ROUND_ZERO_FAST = 'ROUND_ZERO_FAST'
    MIXED_ROUND_ROBIN = 'MIXED_ROUND_ROBIN'


# TODO(mwhittaker): Remove, or pull up so all benchmarks can use.
class ThriftySystemType:
    NOT_THRIFTY = 'NotThrifty'
    RANDOM = 'Random'
    CLOSEST = 'Closest'

# TODO(mwhittaker): Switch from _ms to datetime.
class ElectionOptions(NamedTuple):
    ping_period_ms: float = 1 * 1000
    no_ping_timeout_min_ms: float = 10 * 1000
    no_ping_timeout_max_ms: float = 12 * 1000
    not_enough_votes_timeout_min_ms: float = 10 * 1000
    not_enough_votes_timeout_max_ms: float = 12 * 1000


class HeartbeatOptions(NamedTuple):
    fail_period_ms: float = 1 * 1000
    success_period_ms: float = 2 * 1000
    num_retries: int = 3
    network_delay_alpha: float = 0.9


class AcceptorOptions(NamedTuple):
    wait_period_ms: float = 0
    wait_stagger_ms: float = 0


class LeaderOptions(NamedTuple):
    thrifty_system: str = ThriftySystemType.NOT_THRIFTY
    resend_phase1as_timer_period_ms: float = 5 * 1000
    resend_phase2as_timer_period_ms: float = 5 * 1000
    phase2a_max_buffer_size: int = 1
    phase2a_buffer_flush_period_ms: float = 1000000000
    value_chosen_max_buffer_size: int = 1
    value_chosen_buffer_flush_period_ms: float = 1000000000
    election: ElectionOptions = ElectionOptions()
    heartbeat: HeartbeatOptions = HeartbeatOptions()


class ClientOptions(NamedTuple):
    repropose_period_ms: float = 10 * 1000


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    round_system_type: RoundSystemType
    jvm_heap_size: str

    # Benchmark parameters. ####################################################
    warmup_duration: datetime.timedelta
    warmup_timeout: datetime.timedelta
    warmup_sleep: datetime.timedelta
    duration_seconds: float
    timeout_seconds: float
    client_lag_seconds: float
    state_machine: str
    workload: Workload
    profiled: bool
    monitored: bool
    prometheus_scrape_interval_ms: int

    # Acceptor options. ########################################################
    acceptor: AcceptorOptions = AcceptorOptions()
    acceptor_log_level: str = 'debug'

    # Leader options. ##########################################################
    leader: LeaderOptions = LeaderOptions()
    leader_log_level: str = 'debug'

    # Client options. ##########################################################
    client: ClientOptions = ClientOptions()
    client_log_level: str = 'debug'


Output = benchmark.RecorderOutput


# Network ######################################################################
class FastMultiPaxosNet:
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
        leader_elections: List[host.Endpoint]
        leader_heartbeats: List[host.Endpoint]
        acceptors: List[host.Endpoint]
        acceptor_heartbeats: List[host.Endpoint]

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)
        def portify(hosts: List[host.Host]) -> List[host.Endpoint]:
            return [host.Endpoint(h, next(ports)) for h in hosts]

        def cycle_take_n(n: int, hosts: List[host.Host]) -> List[host.Host]:
            return list(itertools.islice(itertools.cycle(hosts), n))

        return self.Placement(
            leaders = portify(cycle_take_n(
                self._input.f + 1, self._cluster['leaders'])),
            leader_elections = portify(cycle_take_n(
                self._input.f + 1, self._cluster['leaders'])),
            leader_heartbeats = portify(cycle_take_n(
                self._input.f + 1, self._cluster['leaders'])),
            acceptors = portify(cycle_take_n(
                2 * self._input.f + 1, self._cluster['acceptors'])),
            acceptor_heartbeats = portify(cycle_take_n(
                2 * self._input.f + 1, self._cluster['acceptors'])),
            clients = portify(cycle_take_n(
                self._input.num_client_procs, self._cluster['clients'])),
        )

    def config(self) -> proto_util.Message:
        return {
            'f': self._input.f,
            'leaderAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().leaders
            ],
            'leaderElectionAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().leader_elections
            ],
            'leaderHeartbeatAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().leader_heartbeats
            ],
            'acceptorAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().acceptors
            ],
            'acceptorHeartbeatAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.placement().acceptor_heartbeats
            ],
            'roundSystemType': self._input.round_system_type
        }


# Suite ########################################################################
class FastMultiPaxosSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        net = FastMultiPaxosNet(args['cluster'], args['identity_file'], input)
        return self._run_benchmark(bench, args, input, net)

    def _run_benchmark(self,
                       bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any],
                       input: Input,
                       net: FastMultiPaxosNet) -> Output:
        # Write config file.
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(net.config()))
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
        java += [f'-Xms{input.jvm_heap_size}', f'-Xmx{input.jvm_heap_size}']

        # Launch acceptors.
        acceptor_procs = []
        for (i, acceptor) in enumerate(net.placement().acceptors):
            proc = bench.popen(
                host=acceptor.host,
                label=f'acceptor_{i}',
                cmd = java + [
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.fastmultipaxos.AcceptorMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.acceptor_log_level,
                    '--prometheus_host', acceptor.host.ip(),
                    '--prometheus_port',
                        str(acceptor.port + 1) if input.monitored else '-1',
                    '--options.waitPeriod',
                        f'{input.acceptor.wait_period_ms}ms',
                    '--options.waitStagger',
                        f'{input.acceptor.wait_stagger_ms}ms',
                ],
            )
            acceptor_procs.append(proc)
        bench.log('Acceptors started.')

        # Launch leaders.
        leader_procs = []
        for (i, leader) in enumerate(net.placement().leaders):
            proc = bench.popen(
                host=leader.host,
                label=f'leader_{i}',
                cmd = java + [
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.fastmultipaxos.LeaderMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.leader_log_level,
                    '--state_machine', input.state_machine,
                    '--prometheus_host', leader.host.ip(),
                    '--prometheus_port',
                        str(leader.port + 1) if input.monitored else '-1',
                    '--options.thriftySystem',
                        input.leader.thrifty_system,
                    '--options.resendPhase1asTimerPeriod',
                        f'{input.leader.resend_phase1as_timer_period_ms}ms',
                    '--options.resendPhase2asTimerPeriod',
                        f'{input.leader.resend_phase2as_timer_period_ms}ms',
                    '--options.phase2aMaxBufferSize',
                        f'{input.leader.phase2a_max_buffer_size}',
                    '--options.phase2aBufferFlushPeriod',
                        f'{input.leader.phase2a_buffer_flush_period_ms}ms',
                    '--options.valueChosenMaxBufferSize',
                        f'{input.leader.value_chosen_max_buffer_size}',
                    '--options.valueChosenBufferFlushPeriod',
                        f'{input.leader.value_chosen_buffer_flush_period_ms}ms',
                    '--options.election.pingPeriod',
                        f'{input.leader.election.ping_period_ms}ms',
                    '--options.election.noPingTimeoutMin',
                        f'{input.leader.election.no_ping_timeout_min_ms}ms',
                    '--options.election.noPingTimeoutMax',
                        f'{input.leader.election.no_ping_timeout_max_ms}ms',
                    '--options.election.notEnoughVotesTimeoutMin',
                        f'{input.leader.election.not_enough_votes_timeout_min_ms}ms',
                    '--options.election.notEnoughVotesTimeoutMax',
                        f'{input.leader.election.not_enough_votes_timeout_max_ms}ms',
                    '--options.heartbeat.failPeriod',
                        f'{input.leader.heartbeat.fail_period_ms}ms',
                    '--options.heartbeat.successPeriod',
                        f'{input.leader.heartbeat.success_period_ms}ms',
                    '--options.heartbeat.numRetries',
                        str(input.leader.heartbeat.num_retries),
                    '--options.heartbeat.networkDelayAlpha',
                        str(input.leader.heartbeat.network_delay_alpha),
                ],
            )
            leader_procs.append(proc)
        bench.log('Leaders started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                input.prometheus_scrape_interval_ms,
                {
                  'fast_multipaxos_acceptor':
                    [f'{e.host.ip()}:{e.port + 1}'
                     for e in net.placement().acceptors],
                  'fast_multipaxos_leader':
                    [f'{e.host.ip()}:{e.port + 1}'
                     for e in net.placement().leaders],
                  'fast_multipaxos_client':
                    [f'{e.host.ip()}:{e.port + 1}'
                     for e in net.placement().clients],
                }
            )
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.placement().leaders[0].host,
                label='prometheus',
                cmd = [
                    'prometheus',
                    f'--config.file={bench.abspath("prometheus.yml")}',
                    f'--storage.tsdb.path={bench.abspath("prometheus_data")}',
                ],
            )
            bench.log('Prometheus started.')

        # Wait a bit so that a stable leader can elect itself. If we start
        # clients too soon, they may not talk to a stable leader.
        time.sleep(input.client_lag_seconds)
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
                cmd = java + [
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.fastmultipaxos.BenchmarkClientMain',
                    '--host', client.host.ip(),
                    '--port', str(client.port),
                    '--prometheus_host', client.host.ip(),
                    '--prometheus_port',
                        str(client.port + 1) if input.monitored else '-1',
                    '--config', config_filename,
                    '--log_level', input.client_log_level,
                    '--options.reproposePeriod',
                        f'{input.client.repropose_period_ms}ms',
                    '--warmup_duration',
                        f'{input.warmup_duration.total_seconds()}s',
                    '--warmup_timeout',
                        f'{input.warmup_timeout.total_seconds()}s',
                    '--warmup_sleep',
                        f'{input.warmup_sleep.total_seconds()}s',
                    '--num_warmup_clients',
                        f'{input.num_warmup_clients_per_proc}',
                    '--duration', f'{input.duration_seconds}s',
                    '--timeout', f'{input.timeout_seconds}s',
                    '--num_clients', f'{input.num_clients_per_proc}',
                    '--workload', f'{workload_filename}',
                    '--output_file_prefix', bench.abspath(f'client_{i}'),
                ]
            )
            client_procs.append(proc)
        bench.log('Clients started.')

        # Wait for clients to finish and then terminate everything.
        for proc in client_procs:
            proc.wait()
        for proc in leader_procs + acceptor_procs:
            proc.kill()
        if input.monitored:
            prometheus_server.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        return benchmark.parse_recorder_data(bench, client_csvs,
                drop_prefix=datetime.timedelta(seconds=0))


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
