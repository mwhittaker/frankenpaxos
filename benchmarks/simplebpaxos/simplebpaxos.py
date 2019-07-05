from .. import benchmark
from .. import parser_util
from .. import pd_util
from .. import prometheus
from .. import proto_util
from .. import util
from mininet.net import Mininet
from typing import Any, Callable, Collection, Dict, List, NamedTuple
import argparse
import csv
import datetime
import enum
import mininet
import os
import pandas as pd
import subprocess
import time
import tqdm
import yaml


class ClientOptions(NamedTuple):
    repropose_period: datetime.timedelta = datetime.timedelta(milliseconds=100)


class ProposerOptions(NamedTuple):
    resend_phase1as_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_phase2as_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)


class LeaderOptions(NamedTuple):
    resend_dependency_requests_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)


class DepServiceNodeOptions(NamedTuple):
    pass


class AcceptorOptions(NamedTuple):
    pass


class ReplicaOptions(NamedTuple):
    recover_vertex_timer_min_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    recover_vertex_timer_max_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    execute_graph_batch_size: int = 1
    execute_graph_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    # The name of the mininet network.
    net_name: str
    # The maximum number of tolerated faults.
    f: int
    # The number of benchmark client processes launched.
    num_client_procs: int
    # The number of warmup clients run on each benchmark client process.
    num_warmup_clients_per_proc: int
    # The number of clients run on each benchmark client process.
    num_clients_per_proc: int
    # The number of leaders.
    num_leaders: int

    # Benchmark parameters. ####################################################
    # The (rough) duration of the benchmark warmup.
    warmup_duration: datetime.timedelta
    # Global warmup timeout.
    warmup_timeout: datetime.timedelta
    # The (rough) duration of the benchmark.
    duration: datetime.timedelta
    # Global timeout.
    timeout: datetime.timedelta
    # Delay between starting leaders and clients.
    client_lag: datetime.timedelta
    # Profile the code with perf.
    profiled: bool
    # Monitor the code with prometheus.
    monitored: bool
    # The interval between Prometheus scrapes. This field is only relevant if
    # monitoring is enabled.
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
    replica_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str
    client_num_keys: int


Output = benchmark.RecorderOutput


class SimpleBPaxosNet(object):
    def __init__(self) -> None:
        pass

    def __enter__(self) -> 'SimpleBPaxosNet':
        self.net().start()
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        self.net().stop()

    def net(self) -> Mininet:
        raise NotImplementedError()

    def clients(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def leaders(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def proposers(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def dep_service_nodes(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def acceptors(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def replicas(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def config(self) -> proto_util.Message:
        raise NotImplementedError()


class SingleSwitchNet(SimpleBPaxosNet):
    def __init__(self,
                 f: int,
                 num_client_procs: int,
                 num_leaders: int) -> None:
        self.f = f
        self._clients: List[mininet.node.Node] = []
        self._leaders: List[mininet.node.Node] = []
        self._proposers: List[mininet.node.Node] = []
        self._dep_service_nodes: List[mininet.node.Node] = []
        self._acceptors: List[mininet.node.Node] = []
        self._replicas: List[mininet.node.Node] = []
        self._net = Mininet()

        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        for i in range(num_client_procs):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(client)

        for i in range(num_leaders):
            leader = self._net.addHost(f'l{i}')
            self._net.addLink(leader, switch)
            self._leaders.append(leader)

        for i in range(num_leaders):
            proposer = self._net.addHost(f'p{i}')
            self._net.addLink(proposer, switch)
            self._proposers.append(proposer)

        for i in range(2*f + 1):
            dep_service_node = self._net.addHost(f'd{i}')
            self._net.addLink(dep_service_node, switch)
            self._dep_service_nodes.append(dep_service_node)

        for i in range(2*f + 1):
            acceptor = self._net.addHost(f'a{i}')
            self._net.addLink(acceptor, switch)
            self._acceptors.append(acceptor)

        for i in range(f + 1):
            replica = self._net.addHost(f'r{i}')
            self._net.addLink(replica, switch)
            self._replicas.append(replica)

    def net(self) -> Mininet:
        return self._net

    def clients(self) -> List[mininet.node.Node]:
        return self._clients

    def leaders(self) -> List[mininet.node.Node]:
        return self._leaders

    def proposers(self) -> List[mininet.node.Node]:
        return self._proposers

    def dep_service_nodes(self) -> List[mininet.node.Node]:
        return self._dep_service_nodes

    def acceptors(self) -> List[mininet.node.Node]:
        return self._acceptors

    def replicas(self) -> List[mininet.node.Node]:
        return self._replicas

    def config(self) -> proto_util.Message:
        return {
            'f': self.f,
            'leaderAddress': [
                {'host': n.IP(), 'port': 9000}
                for n in self.leaders()
            ],
            'proposerAddress': [
                {'host': n.IP(), 'port': 10000}
                for n in self.proposers()
            ],
            'depServiceNodeAddress': [
                {'host': n.IP(), 'port': 11000}
                for n in self.dep_service_nodes()
            ],
            'acceptorAddress': [
                {'host': n.IP(), 'port': 12000}
                for n in self.acceptors()
            ],
            'replicaAddress': [
                {'host': n.IP(), 'port': 13000}
                for n in self.replicas()
            ],
        }


class SimpleBPaxosSuite(benchmark.Suite[Input, Output]):
    def _run_benchmark(self,
                       bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any],
                       input: Input,
                       net: SimpleBPaxosNet) -> Output:
        # Write config file.
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(net.config()))
        bench.log('Config file config.pbtxt written.')

        # Launch dep service nodes.
        dep_service_node_procs = []
        for (i, host) in enumerate(net.dep_service_nodes()):
            proc = bench.popen(
                f=host.popen,
                label=f'dep_service_node_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.DepServiceNodeMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.dep_service_node_log_level,
                    '--prometheus_host', host.IP(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
                ],
                profile=input.profiled,
            )
            dep_service_node_procs.append(proc)
        bench.log('DepServiceNodes started.')

        # Launch acceptors.
        acceptor_procs = []
        for (i, host) in enumerate(net.acceptors()):
            proc = bench.popen(
                f=host.popen,
                label=f'acceptor_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.AcceptorMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.acceptor_log_level,
                    '--prometheus_host', host.IP(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
                ],
                profile=input.profiled,
            )
            acceptor_procs.append(proc)
        bench.log('Acceptors started.')

        # Launch replicas.
        replica_procs = []
        for (i, host) in enumerate(net.replicas()):
            proc = bench.popen(
                f=host.popen,
                label=f'replica_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.ReplicaMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.replica_log_level,
                    '--prometheus_host', host.IP(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
                    '--options.recoverVertexTimerMinPeriod',
                        '{}s'.format(input.replica_options
                                     .recover_vertex_timer_min_period
                                     .total_seconds()),
                    '--options.recoverVertexTimerMaxPeriod',
                        '{}s'.format(input.replica_options
                                     .recover_vertex_timer_max_period
                                     .total_seconds()),
                    '--options.executeGraphBatchSize',
                        str(input.replica_options.execute_graph_batch_size),
                    '--options.executeGraphTimerPeriod',
                        '{}s'.format(input.replica_options
                                     .execute_graph_timer_period
                                     .total_seconds()),
                ],
                profile=input.profiled,
            )
            replica_procs.append(proc)
        bench.log('Replicas started.')

        # Launch proposers.
        proposer_procs = []
        for (i, host) in enumerate(net.proposers()):
            proc = bench.popen(
                f=host.popen,
                label=f'proposer_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.ProposerMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.proposer_log_level,
                    '--prometheus_host', host.IP(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
                    '--options.resendPhase1asTimerPeriod',
                        '{}s'.format(input.proposer_options
                                     .resend_phase1as_timer_period
                                     .total_seconds()),
                    '--options.resendPhase2asTimerPeriod',
                        '{}s'.format(input.proposer_options
                                     .resend_phase2as_timer_period
                                     .total_seconds()),
                ],
                profile=input.profiled,
            )
            proposer_procs.append(proc)
        bench.log('Proposers started.')

        # Launch leaders.
        leader_procs = []
        for (i, host) in enumerate(net.leaders()):
            proc = bench.popen(
                f=host.popen,
                label=f'leader_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.LeaderMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.leader_log_level,
                    '--prometheus_host', host.IP(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
                    '--options.resendDependencyRequestsTimerPeriod',
                        '{}s'.format(input.leader_options
                                     .resend_dependency_requests_timer_period
                                     .total_seconds()),
                ],
                profile=input.profiled,
            )
            leader_procs.append(proc)
        bench.log('Leaders started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000),
                {
                  'bpaxos_leader':
                    [f'{n.IP()}:12345' for n in net.leaders()],
                  'bpaxos_proposer':
                    [f'{n.IP()}:12345' for n in net.proposers()],
                  'bpaxos_acceptor':
                    [f'{n.IP()}:12345' for n in net.acceptors()],
                  'bpaxos_client':
                    [f'{n.IP()}:12345' for n in net.clients()],
                  'bpaxos_dep_service_node':
                    [f'{n.IP()}:12345' for n in net.dep_service_nodes()],
                  'bpaxos_replica':
                    [f'{n.IP()}:12345' for n in net.replicas()],
                }
            )
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                f=net.leaders()[0].popen,
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
        client_procs = []
        for (i, host) in enumerate(net.clients()):
            proc = bench.popen(
                f=host.popen,
                label=f'client_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.BenchmarkClientMain',
                    '--host', host.IP(),
                    '--port', str(10000),
                    '--config', config_filename,
                    '--log_level', input.client_log_level,
                    '--prometheus_host', host.IP(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
                    '--warmup_duration',
                        f'{input.warmup_duration.total_seconds()}s',
                    '--warmup_timeout',
                        f'{input.warmup_timeout.total_seconds()}s',
                    '--num_warmup_clients',
                        f'{input.num_warmup_clients_per_proc}',
                    '--duration', f'{input.duration.total_seconds()}s',
                    '--timeout', f'{input.timeout.total_seconds()}s',
                    '--num_clients', f'{input.num_clients_per_proc}',
                    '--num_keys', f'{input.client_num_keys}',
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
        for proc in (leader_procs + proposer_procs + acceptor_procs +
                     dep_service_node_procs + replica_procs):
            proc.terminate()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        return benchmark.parse_recorder_data(bench, client_csvs)

    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        with SingleSwitchNet(f=input.f,
                             num_client_procs=input.num_client_procs,
                             num_leaders=input.num_leaders) as net:
            return self._run_benchmark(bench, args, input, net)


def _main(args) -> None:
    class ExampleSimpleBPaxosSuite(SimpleBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    net_name = 'SingleSwitchNet',
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = 1,
                    num_leaders = 2,
                    warmup_duration = datetime.timedelta(seconds=5),
                    warmup_timeout = datetime.timedelta(seconds=10),
                    duration = datetime.timedelta(seconds=10),
                    timeout = datetime.timedelta(seconds=30),
                    client_lag = datetime.timedelta(seconds=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    dep_service_node_options = DepServiceNodeOptions(),
                    dep_service_node_log_level = 'debug',
                    leader_options = LeaderOptions(),
                    leader_log_level = 'debug',
                    proposer_options = ProposerOptions(),
                    proposer_log_level = 'debug',
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = 'debug',
                    replica_options = ReplicaOptions(
                        execute_graph_batch_size = 1,
                    ),
                    replica_log_level = 'debug',
                    client_options = ClientOptions(),
                    client_log_level = 'debug',
                    client_num_keys = 100,
                )
                for num_client_procs in [1, 2]
            ] * 2

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_client_procs': input.num_client_procs,
                'output.throughput_1s.p90': f'{output.throughput_1s.p90}',
            })

    suite = ExampleSimpleBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'simplebpaxos') as dir:
        suite.run_suite(dir)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()


if __name__ == '__main__':
    _main(get_parser().parse_args())
