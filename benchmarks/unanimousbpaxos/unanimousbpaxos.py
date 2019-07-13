from .. import benchmark
from .. import host
from .. import parser_util
from .. import pd_util
from .. import prometheus
from .. import proto_util
from .. import util
from typing import Any, Callable, Collection, Dict, List, NamedTuple
import argparse
import csv
import datetime
import enum
import itertools
import mininet
import mininet.net
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
    # The maximum number of tolerated faults.
    f: int
    # The number of benchmark client processes launched.
    num_client_procs: int
    # The number of clients run on each benchmark client process.
    num_clients_per_proc: int

    # Benchmark parameters. ####################################################
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

    # Dep service node options. ################################################
    dep_service_node_options: DepServiceNodeOptions
    dep_service_node_log_level: str

    # Acceptor options. ########################################################
    acceptor_options: AcceptorOptions
    acceptor_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str
    client_num_keys: int


Output = benchmark.RecorderOutput


# Networks #####################################################################
class UnanimousBPaxosNet(object):
    def __init__(self) -> None:
        pass

    def __enter__(self) -> 'UnanimousBPaxosNet':
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        pass

    def f(self) -> int:
        raise NotImplementedError()

    def clients(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def leaders(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def dep_service_nodes(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def acceptors(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def config(self) -> proto_util.Message:
        return {
            'f': self.f(),
            'leaderAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.leaders()
            ],
            'depServiceNodeAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.dep_service_nodes()
            ],
            'acceptorAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.acceptors()
            ],
        }

class RemoteUnanimousBPaxosNet(UnanimousBPaxosNet):
    def __init__(self,
                 addresses: List[str],
                 f: int,
                 num_client_procs: int) -> None:
        assert len(addresses) > 0
        self._f = f
        self._num_client_procs = num_client_procs
        self._hosts = [self._make_host(a) for a in addresses]

    def _make_host(self, address: str) -> host.Host:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
        client.connect(address)
        return host.RemoteHost(client)

    class _Placement(NamedTuple):
        clients: List[host.Endpoint]
        leaders: List[host.Endpoint]
        dep_service_nodes: List[host.Endpoint]
        acceptors: List[host.Endpoint]

    def _placement(self) -> '_Placement':
        ports = itertools.count(10000, 100)
        def portify(hosts: List[host.Host]) -> List[host.Endpoint]:
            return [host.Endpoint(h, next(ports)) for h in hosts]

        if len(self._hosts) == 1:
            return self._Placement(
                clients = portify(self._hosts * self._num_client_procs),
                leaders = portify(self._hosts * (self.f() + 1)),
                dep_service_nodes = portify(self._hosts * (2*self.f() + 1)),
                acceptors = portify(self._hosts * (2*self.f() + 1)),
            )
        else:
            raise NotImplementedError()

    def f(self) -> int:
        return self._f

    def clients(self) -> List[host.Endpoint]:
        return self._placement().clients

    def leaders(self) -> List[host.Endpoint]:
        return self._placement().leaders

    def dep_service_nodes(self) -> List[host.Endpoint]:
        return self._placement().dep_service_nodes

    def acceptors(self) -> List[host.Endpoint]:
        return self._placement().acceptors


class UnanimousBPaxosMininet(UnanimousBPaxosNet):
    def __enter__(self) -> 'UnanimousBPaxosMininet':
        self.net().start()
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        self.net().stop()

    def net(self) -> mininet.net.Mininet:
        raise NotImplementedError()


class SingleSwitchMininet(UnanimousBPaxosMininet):
    def __init__(self, f: int,
                 num_client_procs: int) -> None:
        self._f = f
        self._clients: List[host.Endpoint] = []
        self._leaders: List[host.Endpoint] = []
        self._dep_service_nodes: List[host.Endpoint] = []
        self._acceptors: List[host.Endpoint] = []
        self._net = mininet.net.Mininet()

        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        for i in range(num_client_procs):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(host.Endpoint(host.MininetHost(client), 10000))

        for i in range(f + 1):
            leader = self._net.addHost(f'l{i}')
            self._net.addLink(leader, switch)
            self._leaders.append(host.Endpoint(host.MininetHost(leader), 11000))

        for i in range(2*f + 1):
            dep_service_node = self._net.addHost(f'd{i}')
            self._net.addLink(dep_service_node, switch)
            self._dep_service_nodes.append(
                host.Endpoint(host.MininetHost(dep_service_node), 12000))

        for i in range(2*f + 1):
            acceptor = self._net.addHost(f'a{i}')
            self._net.addLink(acceptor, switch)
            self._acceptors.append(
                host.Endpoint(host.MininetHost(acceptor), 13000))

    def f(self) -> int:
        return self._f

    def net(self) -> mininet.net.Mininet:
        return self._net

    def clients(self) -> List[host.Endpoint]:
        return self._clients

    def leaders(self) -> List[host.Endpoint]:
        return self._leaders

    def dep_service_nodes(self) -> List[host.Endpoint]:
        return self._dep_service_nodes

    def acceptors(self) -> List[host.Endpoint]:
        return self._acceptors


# Suite ########################################################################
class UnanimousBPaxosSuite(benchmark.Suite[Input, Output]):
    def make_net(self,
                 args: Dict[Any, Any],
                 input: Input) -> UnanimousBPaxosNet:
        if args['address'] is not None:
            return RemoteUnanimousBPaxosNet(
                        args['address'],
                        f=input.f,
                        num_client_procs=input.num_client_procs)
        else:
            return SingleSwitchMininet(
                        f=input.f,
                        num_client_procs=input.num_client_procs)

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
                       net: UnanimousBPaxosNet) -> Output:
        # Write config file.
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(net.config()))
        bench.log('Config file config.pbtxt written.')

        # Launch leaders.
        leader_procs = []
        for (i, leader) in enumerate(net.leaders()):
            proc = bench.popen(
                host=leader.host,
                label=f'leader_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.unanimousbpaxos.LeaderMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.leader_log_level,
                    '--prometheus_host', leader.host.ip(),
                    '--prometheus_port',
                        str(leader.port + 1) if input.monitored else '-1',
                    '--options.resendDependencyRequestsTimerPeriod',
                        '{}s'.format(input.leader_options
                                     .resend_dependency_requests_timer_period
                                     .total_seconds()),
                    '--options.resendPhase1asTimerPeriod',
                        '{}s'.format(input.leader_options
                                     .resend_phase1as_timer_period
                                     .total_seconds()),
                    '--options.resendPhase2asTimerPeriod',
                        '{}s'.format(input.leader_options
                                     .resend_phase2as_timer_period
                                     .total_seconds()),
                    '--options.recoverVertexTimerMinPeriod',
                        '{}s'.format(input.leader_options
                                     .recover_vertex_timer_min_period
                                     .total_seconds()),
                    '--options.recoverVertexTimerMaxPeriod',
                        '{}s'.format(input.leader_options
                                     .recover_vertex_timer_max_period
                                     .total_seconds()),
                ],
            )
            leader_procs.append(proc)
        bench.log('Leaders started.')

        # Launch acceptors.
        acceptor_procs = []
        for (i, acceptor) in enumerate(net.acceptors()):
            proc = bench.popen(
                host=acceptor.host,
                label=f'acceptor_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.unanimousbpaxos.AcceptorMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.acceptor_log_level,
                    '--prometheus_host', acceptor.host.ip(),
                    '--prometheus_port',
                        str(acceptor.port + 1) if input.monitored else '-1',
                ],
            )
            acceptor_procs.append(proc)
        bench.log('Acceptors started.')

        # Launch dep service nodes.
        dep_service_node_procs = []
        for (i, dep) in enumerate(net.dep_service_nodes()):
            proc = bench.popen(
                host=dep.host,
                label=f'dep_service_node_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.unanimousbpaxos.DepServiceNodeMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.dep_service_node_log_level,
                    '--prometheus_host', dep.host.ip(),
                    '--prometheus_port',
                        str(dep.port + 1) if input.monitored else '-1',
                ],
            )
            dep_service_node_procs.append(proc)
        bench.log('DepServiceNodes started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000),
                {
                  'bpaxos_leader':
                    [f'{e.host.ip()}:{e.port + 1}'
                     for e in net.leaders()],
                  'bpaxos_acceptor':
                    [f'{e.host.ip()}:{e.port + 1}'
                     for e in net.acceptors()],
                  'bpaxos_client':
                    [f'{e.host.ip()}:{e.port + 1}'
                     for e in net.clients()],
                  'bpaxos_dep_service_node':
                    [f'{e.host.ip()}:{e.port + 1}'
                     for e in net.dep_service_nodes()],
                }
            )
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.leaders()[0].host,
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
        for (i, client) in enumerate(net.clients()):
            proc = bench.popen(
                host=client.host,
                label=f'client_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.unanimousbpaxos.BenchmarkClientMain',
                    '--host', client.host.ip(),
                    '--port', str(client.port),
                    '--config', config_filename,
                    '--log_level', input.client_log_level,
                    '--prometheus_host', client.host.ip(),
                    '--prometheus_port',
                        str(client.port + 1) if input.monitored else '-1',
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
        for proc in leader_procs + acceptor_procs + dep_service_node_procs:
            proc.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        # TODO(mwhittaker): Add warmup.
        return benchmark.parse_recorder_data(bench, client_csvs,
                drop_prefix=datetime.timedelta(seconds=0))


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
