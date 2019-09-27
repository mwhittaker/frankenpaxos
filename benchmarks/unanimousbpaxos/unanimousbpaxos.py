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
from typing import Any, Callable, Collection, Dict, List, NamedTuple, Optional
import argparse
import csv
import datetime
import enum
import itertools
import os
import paramiko
import time
import yaml

# Input/Output #################################################################


class ClientOptions(NamedTuple):
    repropose_period: datetime.timedelta = datetime.timedelta(milliseconds=100)


class LeaderOptions(NamedTuple):
    resend_dependency_requests_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_phase1as_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_phase2as_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    recover_vertex_timer_min_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    recover_vertex_timer_max_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)


class DepServiceNodeOptions(NamedTuple):
    pass


class AcceptorOptions(NamedTuple):
    pass


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    num_client_procs: int
    num_clients_per_proc: int

    # Benchmark parameters. ####################################################
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
    leader_dependency_graph: str

    # Dep service node options. ################################################
    dep_service_node_options: DepServiceNodeOptions
    dep_service_node_log_level: str

    # Acceptor options. ########################################################
    acceptor_options: AcceptorOptions
    acceptor_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str


Output = benchmark.RecorderOutput


# Networks #####################################################################
class UnanimousBPaxosNet:
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
        client.connect(address)
        return host.RemoteHost(client)

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        leaders: List[host.Endpoint]
        dep_service_nodes: List[host.Endpoint]
        acceptors: List[host.Endpoint]

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
                cycle_take_n(self._input.f + 1, self._cluster['leaders'])),
            dep_service_nodes=portify(
                cycle_take_n(2 * self._input.f + 1,
                             self._cluster['dep_service_nodes'])),
            acceptors=portify(
                cycle_take_n(2 * self._input.f + 1,
                             self._cluster['acceptors'])),
        )

    def config(self) -> proto_util.Message:
        return {
            'f':
                self._input.f,
            'leaderAddress': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().leaders],
            'depServiceNodeAddress': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().dep_service_nodes],
            'acceptorAddress': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().acceptors],
        }


# Suite ########################################################################
class UnanimousBPaxosSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], input: Input) -> Output:
        net = UnanimousBPaxosNet(args['cluster'], args['identity_file'], input)
        return self._run_benchmark(bench, args, input, net)

    def _run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any], input: Input,
                       net: UnanimousBPaxosNet) -> Output:
        # Write config file.
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(net.config()))
        bench.log('Config file config.pbtxt written.')

        # Launch leaders.
        leader_procs = []
        for (i, leader) in enumerate(net.placement().leaders):
            proc = bench.popen(
                host=leader.host,
                label=f'leader_{i}',
                cmd=[
                    'java',
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.unanimousbpaxos.LeaderMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.leader_log_level,
                    '--state_machine',
                    input.state_machine,
                    '--dependency_graph',
                    input.leader_dependency_graph,
                    '--prometheus_host',
                    leader.host.ip(),
                    '--prometheus_port',
                    str(leader.port + 1) if input.monitored else '-1',
                    '--options.resendDependencyRequestsTimerPeriod',
                    '{}s'.format(input.leader_options.
                                 resend_dependency_requests_timer_period.
                                 total_seconds()),
                    '--options.resendPhase1asTimerPeriod',
                    '{}s'.format(input.leader_options.
                                 resend_phase1as_timer_period.total_seconds()),
                    '--options.resendPhase2asTimerPeriod',
                    '{}s'.format(input.leader_options.
                                 resend_phase2as_timer_period.total_seconds()),
                    '--options.recoverVertexTimerMinPeriod',
                    '{}s'.format(
                        input.leader_options.recover_vertex_timer_min_period.
                        total_seconds()),
                    '--options.recoverVertexTimerMaxPeriod',
                    '{}s'.format(
                        input.leader_options.recover_vertex_timer_max_period.
                        total_seconds()),
                ],
            )
            leader_procs.append(proc)
        bench.log('Leaders started.')

        # Launch acceptors.
        acceptor_procs = []
        for (i, acceptor) in enumerate(net.placement().acceptors):
            proc = bench.popen(
                host=acceptor.host,
                label=f'acceptor_{i}',
                cmd=[
                    'java',
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.unanimousbpaxos.AcceptorMain',
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
            acceptor_procs.append(proc)
        bench.log('Acceptors started.')

        # Launch dep service nodes.
        dep_service_node_procs = []
        for (i, dep) in enumerate(net.placement().dep_service_nodes):
            proc = bench.popen(
                host=dep.host,
                label=f'dep_service_node_{i}',
                cmd=[
                    'java',
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.unanimousbpaxos.DepServiceNodeMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.dep_service_node_log_level,
                    '--state_machine',
                    input.state_machine,
                    '--prometheus_host',
                    dep.host.ip(),
                    '--prometheus_port',
                    str(dep.port + 1) if input.monitored else '-1',
                ],
            )
            dep_service_node_procs.append(proc)
        bench.log('DepServiceNodes started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'bpaxos_leader': [
                        f'{e.host.ip()}:{e.port + 1}'
                        for e in net.placement().leaders
                    ],
                    'bpaxos_acceptor': [
                        f'{e.host.ip()}:{e.port + 1}'
                        for e in net.placement().acceptors
                    ],
                    'bpaxos_client': [
                        f'{e.host.ip()}:{e.port + 1}'
                        for e in net.placement().clients
                    ],
                    'bpaxos_dep_service_node': [
                        f'{e.host.ip()}:{e.port + 1}'
                        for e in net.placement().dep_service_nodes
                    ],
                })
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.placement().leaders[0].host,
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

        client_procs = []
        for (i, client) in enumerate(net.placement().clients):
            proc = bench.popen(
                host=client.host,
                label=f'client_{i}',
                cmd=[
                    'java',
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.unanimousbpaxos.BenchmarkClientMain',
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
                    '--options.reproposePeriod',
                    '{}s'.format(
                        input.client_options.repropose_period.total_seconds()),
                ])
            client_procs.append(proc)
        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for clients to finish and then terminate leaders and acceptors.
        for proc in client_procs:
            proc.wait()
        for proc in leader_procs + acceptor_procs + dep_service_node_procs:
            proc.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [
            bench.abspath(f'client_{i}_data.csv')
            for i in range(input.num_client_procs)
        ]
        # TODO(mwhittaker): Add warmup.
        return benchmark.parse_recorder_data(
            bench, client_csvs, drop_prefix=datetime.timedelta(seconds=0))


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
