from .. import benchmark
from .. import host
from .. import parser_util
from .. import pd_util
from .. import proc
from .. import prometheus
from .. import proto_util
from .. import util
from typing import Any, Callable, Collection, Dict, List, NamedTuple
import argparse
import csv
import datetime
import enum
import mininet
import mininet.net
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
    # Warmup timeout.
    warmup_timeout: datetime.timedelta
    # Warmup sleep time.
    warmup_sleep: datetime.timedelta
    # The (rough) duration of the benchmark.
    duration: datetime.timedelta
    # Benchmark timeout.
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


# Networks #####################################################################
class SimpleBPaxosNet(object):
    def __init__(self) -> None:
        pass

    def __enter__(self) -> 'SimpleBPaxosNet':
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        pass

    def f(self) -> int:
        raise NotImplementedError()

    def clients(self) -> List[host.Host]:
        raise NotImplementedError()

    def leaders(self) -> List[host.Host]:
        raise NotImplementedError()

    def proposers(self) -> List[host.Host]:
        raise NotImplementedError()

    def dep_service_nodes(self) -> List[host.Host]:
        raise NotImplementedError()

    def acceptors(self) -> List[host.Host]:
        raise NotImplementedError()

    def replicas(self) -> List[host.Host]:
        raise NotImplementedError()

    def config(self) -> proto_util.Message:
        return {
            'f': self.f(),
            'leaderAddress': [
                {'host': h.ip(), 'port': 9000 + i}
                for (i, h) in enumerate(self.leaders())
            ],
            'proposerAddress': [
                {'host': h.ip(), 'port': 10000 + i}
                for (i, h) in enumerate(self.proposers())
            ],
            'depServiceNodeAddress': [
                {'host': h.ip(), 'port': 11000 + i}
                for (i, h) in enumerate(self.dep_service_nodes())
            ],
            'acceptorAddress': [
                {'host': h.ip(), 'port': 12000 + i}
                for (i, h) in enumerate(self.acceptors())
            ],
            'replicaAddress': [
                {'host': h.ip(), 'port': 13000 + i}
                for (i, h) in enumerate(self.replicas())
            ],
        }

class RemoteSimpleBPaxosNet(SimpleBPaxosNet):
    def __init__(self,
                 addresses: List[str],
                 f: int,
                 num_client_procs: int,
                 num_leaders: int) -> None:
        assert len(addresses) > 0
        self._f = f
        self._num_client_procs = num_client_procs
        self._num_leaders = num_leaders
        self._hosts = [self._make_host(a) for a in addresses]

    def _make_host(self, address: str) -> host.Host:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
        client.connect(address, username='vagrant', password='vagrant')
        return host.RemoteHost(client)

    class _Placement(NamedTuple):
        clients: List[host.Host]
        leaders: List[host.Host]
        proposers: List[host.Host]
        dep_service_nodes: List[host.Host]
        acceptors: List[host.Host]
        replicas: List[host.Host]

    def _placement(self) -> '_Placement':
        if len(self._hosts) == 1:
            return self._Placement(
                clients = self._hosts * self._num_client_procs,
                leaders = self._hosts * self._num_leaders,
                proposers = self._hosts * self._num_leaders,
                dep_service_nodes = self._hosts * (2*self.f() + 1),
                acceptors = self._hosts * (2*self.f() + 1),
                replicas = self._hosts * (self.f() + 1),
            )
        elif len(self._hosts) == 4:
            return self._Placement(
                clients = [self._hosts[0]] * self._num_client_procs,
                leaders = [self._hosts[1]] * self._num_leaders,
                proposers = [self._hosts[1]] * self._num_leaders,
                dep_service_nodes = [self._hosts[2]] * (2*self.f() + 1),
                acceptors = [self._hosts[2]] * (2*self.f() + 1),
                replicas = [self._hosts[3]] * (self.f() + 1),
            )
        else:
            raise NotImplementedError()

    def f(self) -> int:
        return self._f

    def clients(self) -> List[host.Host]:
        return self._placement().clients

    def leaders(self) -> List[host.Host]:
        return self._placement().leaders

    def proposers(self) -> List[host.Host]:
        return self._placement().proposers

    def dep_service_nodes(self) -> List[host.Host]:
        return self._placement().dep_service_nodes

    def acceptors(self) -> List[host.Host]:
        return self._placement().acceptors

    def replicas(self) -> List[host.Host]:
        return self._placement().replicas


class SimpleBPaxosMininet(SimpleBPaxosNet):
    def __enter__(self) -> 'SimpleBPaxosMininet':
        self.net().start()
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        self.net().stop()

    def net(self) -> mininet.net.Mininet:
        raise NotImplementedError()


class SingleSwitchMininet(SimpleBPaxosMininet):
    def __init__(self,
                 f: int,
                 num_client_procs: int,
                 num_leaders: int) -> None:
        self._f = f
        self._clients: List[host.Host] = []
        self._leaders: List[host.Host] = []
        self._proposers: List[host.Host] = []
        self._dep_service_nodes: List[host.Host] = []
        self._acceptors: List[host.Host] = []
        self._replicas: List[host.Host] = []
        self._net = mininet.net.Mininet()

        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        for i in range(num_client_procs):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(host.MininetHost(client))

        for i in range(num_leaders):
            leader = self._net.addHost(f'l{i}')
            self._net.addLink(leader, switch)
            self._leaders.append(host.MininetHost(leader))

        for i in range(num_leaders):
            proposer = self._net.addHost(f'p{i}')
            self._net.addLink(proposer, switch)
            self._proposers.append(host.MininetHost(proposer))

        for i in range(2*f + 1):
            dep_service_node = self._net.addHost(f'd{i}')
            self._net.addLink(dep_service_node, switch)
            self._dep_service_nodes.append(host.MininetHost(dep_service_node))

        for i in range(2*f + 1):
            acceptor = self._net.addHost(f'a{i}')
            self._net.addLink(acceptor, switch)
            self._acceptors.append(host.MininetHost(acceptor))

        for i in range(f + 1):
            replica = self._net.addHost(f'r{i}')
            self._net.addLink(replica, switch)
            self._replicas.append(host.MininetHost(replica))

    def net(self) -> mininet.net.Mininet:
        return self._net

    def clients(self) -> List[host.Host]:
        return self._clients

    def leaders(self) -> List[host.Host]:
        return self._leaders

    def proposers(self) -> List[host.Host]:
        return self._proposers

    def dep_service_nodes(self) -> List[host.Host]:
        return self._dep_service_nodes

    def acceptors(self) -> List[host.Host]:
        return self._acceptors

    def replicas(self) -> List[host.Host]:
        return self._replicas


# Suite ########################################################################
class SimpleBPaxosSuite(benchmark.Suite[Input, Output]):
    def make_net(self, args: Dict[Any, Any], input: Input) -> SimpleBPaxosNet:
        raise NotImplementedError

    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        with self.make_net(args, input) as net:
            return self._run_benchmark(bench, args, input, net)

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
                host=host,
                label=f'dep_service_node_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.DepServiceNodeMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.dep_service_node_log_level,
                    '--prometheus_host', host.ip(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
                ],
            )
            dep_service_node_procs.append(proc)
        bench.log('DepServiceNodes started.')

        # Launch acceptors.
        acceptor_procs = []
        for (i, host) in enumerate(net.acceptors()):
            proc = bench.popen(
                host=host,
                label=f'acceptor_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.AcceptorMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.acceptor_log_level,
                    '--prometheus_host', host.ip(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
                ],
            )
            acceptor_procs.append(proc)
        bench.log('Acceptors started.')

        # Launch replicas.
        replica_procs = []
        for (i, host) in enumerate(net.replicas()):
            proc = bench.popen(
                host=host,
                label=f'replica_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.ReplicaMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.replica_log_level,
                    '--prometheus_host', host.ip(),
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
            )
            replica_procs.append(proc)
        bench.log('Replicas started.')

        # Launch proposers.
        proposer_procs = []
        for (i, host) in enumerate(net.proposers()):
            proc = bench.popen(
                host=host,
                label=f'proposer_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.ProposerMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.proposer_log_level,
                    '--prometheus_host', host.ip(),
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
            )
            proposer_procs.append(proc)
        bench.log('Proposers started.')

        # Launch leaders.
        leader_procs = []
        for (i, host) in enumerate(net.leaders()):
            proc = bench.popen(
                host=host,
                label=f'leader_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.LeaderMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.leader_log_level,
                    '--prometheus_host', host.ip(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
                    '--options.resendDependencyRequestsTimerPeriod',
                        '{}s'.format(input.leader_options
                                     .resend_dependency_requests_timer_period
                                     .total_seconds()),
                ],
            )
            leader_procs.append(proc)
        bench.log('Leaders started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000),
                {
                  'bpaxos_leader':
                    [f'{n.ip()}:12345' for n in net.leaders()],
                  'bpaxos_proposer':
                    [f'{n.ip()}:12345' for n in net.proposers()],
                  'bpaxos_acceptor':
                    [f'{n.ip()}:12345' for n in net.acceptors()],
                  'bpaxos_client':
                    [f'{n.ip()}:12345' for n in net.clients()],
                  'bpaxos_dep_service_node':
                    [f'{n.ip()}:12345' for n in net.dep_service_nodes()],
                  'bpaxos_replica':
                    [f'{n.ip()}:12345' for n in net.replicas()],
                }
            )
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.clients()[0],
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
                host=host,
                label=f'client_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.BenchmarkClientMain',
                    '--host', host.ip(),
                    '--port', str(10000 + i),
                    '--config', config_filename,
                    '--log_level', input.client_log_level,
                    '--prometheus_host', host.ip(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
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
            proc.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        return benchmark.parse_recorder_data(bench, client_csvs,
                drop_prefix=input.warmup_duration + input.warmup_sleep)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
