from .. import benchmark
from .. import host
from .. import parser_util
from .. import pd_util
from .. import prometheus
from .. import proto_util
from .. import util
from .. import workload
from ..workload import Workload
from typing import Any, Callable, Dict, List, NamedTuple
import argparse
import csv
import datetime
import enum
import itertools
import mininet
import mininet.net
import os
import pandas as pd
import paramiko
import subprocess
import time
import yaml


# Input/Output #################################################################
class RoundSystemType(enum.Enum):
    CLASSIC_ROUND_ROBIN = 'CLASSIC_ROUND_ROBIN'

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

class ProposerOptions(NamedTuple):
    pass

class ExecutorOptions(NamedTuple):
    wait_period_ms: float = 0
    wait_stagger_ms: float = 0


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    # The maximum number of tolerated faults.
    f: int
    # The number of benchmark client processes launched.
    num_client_procs: int
    # The number of clients run on each benchmark client process.
    num_clients_per_proc: int
    # The type of round system used by the protocol.
    round_system_type: RoundSystemType

    # Benchmark parameters. ####################################################
    # The (rough) duration of the benchmark.
    duration_seconds: float
    # Global timeout.
    timeout_seconds: float
    # Delay between starting leaders and clients.
    client_lag_seconds: float
    # State machine
    state_machine: str
    # Client workload.
    workload: Workload
    # Profile the code with perf.
    profiled: bool
    # Monitor the code with prometheus.
    monitored: bool
    # The interval between Prometheus scrapes. This field is only relevant if
    # monitoring is enabled.
    prometheus_scrape_interval_ms: int

    # Acceptor options. ########################################################
    acceptor: AcceptorOptions = AcceptorOptions()

    # Leader options. ##########################################################
    leader: LeaderOptions = LeaderOptions()
    leader_log_level: str = 'debug'

    # Client options. ##########################################################
    client: ClientOptions = ClientOptions()

    # Proposer options
    proposer: ProposerOptions = ProposerOptions()

    # Executor options
    executor: ExecutorOptions = ExecutorOptions()


Output = benchmark.RecorderOutput


# Networks #####################################################################
class SPaxosDecoupleNet(object):
    def __init__(self) -> None:
        pass

    def __enter__(self) -> 'SPaxosDecoupleNet':
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        pass

    def f(self) -> int:
        raise NotImplementedError()

    def rs_type(self) -> RoundSystemType:
        raise NotImplementedError()

    def clients(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def leaders(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def leader_elections(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def leader_heartbeats(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def acceptors(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def acceptor_heartbeats(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def fake_leaders(self) -> List[host.Endpoint]:
        raise NotImplementedError()

    def config(self) -> proto_util.Message:
        return {
            'f': self.f(),
            'proposerAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.proposers()
            ],
            'leaderAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.leaders()
            ],
            'leaderElectionAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.leader_elections()
            ],
            'leaderHeartbeatAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.leader_heartbeats()
            ],
            'acceptorAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.acceptors()
            ],
            'acceptorHeartbeatAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.acceptor_heartbeats()
            ],
            'executorAddress': [
                {'host': e.host.ip(), 'port': e.port}
                for e in self.executors()
            ],
            'fakeLeaderAddress': [
                {'host': e.host.ip(), 'port': e.port}
            ],
            'roundSystemType': self.rs_type()
        }


class SPaxosDecoupleNet(SPaxosDecoupleNet):
    def __init__(self,
                 addresses: List[str],
                 f: int,
                 rs_type: RoundSystemType,
                 num_client_procs: int) -> None:
        assert len(addresses) > 0
        self._f = f
        self._rs_type = rs_type
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
        leader_elections: List[host.Endpoint]
        leader_heartbeats: List[host.Endpoint]
        acceptors: List[host.Endpoint]
        acceptor_heartbeats: List[host.Endpoint]
        proposers: List[host.Endpoint]
        executors: List[host.Endpoint]
        fake_leaders: List[host.Endpoint]

    def _placement(self) -> '_Placement':
        ports = itertools.count(10000, 100)
        def portify(hosts: List[host.Host]) -> List[host.Endpoint]:
            return [host.Endpoint(h, next(ports)) for h in hosts]

        if len(self._hosts) == 1:
            return self._Placement(
                clients = portify(self._hosts * self._num_client_procs),
                leaders = portify(self._hosts * (1)),
                leader_elections = portify(self._hosts * (1)),
                leader_heartbeats = portify(self._hosts * (1)),
                acceptors = portify(self._hosts * (2*self.f() + 1)),
                acceptor_heartbeats = portify(self._hosts * (2*self.f() + 1)),
                proposers = portify(self._hosts * (self.f() + 1)),
                executors = portify(self._hosts * (self.f() + 1)),
                fake_leaders = portify(self._hosts * (self.f() + 1))
            )
        else:
            raise NotImplementedError()

    def f(self) -> int:
        return self._f

    def rs_type(self) -> RoundSystemType:
        return self._rs_type

    def clients(self) -> List[host.Endpoint]:
        return self._placement().clients

    def leaders(self) -> List[host.Endpoint]:
        return self._placement().leaders

    def leader_elections(self) -> List[host.Endpoint]:
        return self._placement().leader_elections

    def leader_heartbeats(self) -> List[host.Endpoint]:
        return self._placement().leader_heartbeats

    def acceptors(self) -> List[host.Endpoint]:
        return self._placement().acceptors

    def acceptor_heartbeats(self) -> List[host.Endpoint]:
        return self._placement().acceptor_heartbeats

    def proposers(self) -> List[host.Endpoint]:
        return self._placement().proposers

    def executors(self) -> List[host.Endpoint]:
        return self._placement().executors

    def fake_leaders(self) -> List[host.Endpoint]:
        return self._placement().fake_leaders


class SPaxosDecoupleMininet(SPaxosDecoupleNet):
    def __enter__(self) -> 'SPaxosDecoupleNet':
        self.net().start()
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        self.net().stop()

    def net(self) -> mininet.net.Mininet:
        raise NotImplementedError()


class SingleSwitchMininet(SPaxosDecoupleMininet):
    def __init__(self,
                 f: int,
                 rs_type: RoundSystemType,
                 num_client_procs: int) -> None:
        self._f = f
        self._rs_type = rs_type
        self._clients: List[host.Endpoint] = []
        self._leaders: List[host.Endpoint] = []
        self._leader_elections: List[host.Endpoint] = []
        self._leader_heartbeats: List[host.Endpoint] = []
        self._acceptors: List[host.Endpoint] = []
        self._acceptor_heartbeats: List[host.Endpoint] = []
        self._proposers: List[host.Endpoint] = []
        self._executors: List[host.Endpoint] = []
        self._fake_leaders: List[host.Endpoint] = []
        self._net = mininet.net.Mininet()

        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        for i in range(num_client_procs):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(host.Endpoint(host.MininetHost(client), 10000))

        num_leaders = 1
        for i in range(num_leaders):
            leader = self._net.addHost(f'l{i}')
            self._net.addLink(leader, switch)
            self._leaders.append(
                host.Endpoint(host.MininetHost(leader), 11000))
            self._leader_elections.append(
                host.Endpoint(host.MininetHost(leader), 11010))
            self._leader_heartbeats.append(
                host.Endpoint(host.MininetHost(leader), 11020))

        num_acceptors = 2*f + 1
        for i in range(num_acceptors):
            acceptor = self._net.addHost(f'a{i}')
            self._net.addLink(acceptor, switch)
            self._acceptors.append(
                host.Endpoint(host.MininetHost(acceptor), 12000))
            self._acceptor_heartbeats.append(
                host.Endpoint(host.MininetHost(acceptor), 12010))

        num_proposers = f + 1
        for i in range(num_proposers):
            proposer = self._net.addHost(f'p{i}')
            self._net.addLink(proposer, switch)
            self._proposers.append(
                host.Endpoint(host.MininetHost(proposer), 13000))

        num_executors = f + 1
        for i in range(num_executors):
            executor = self._net.addHost(f'e{i}')
            self._net.addLink(executor, switch)
            self._executors.append(
                host.Endpoint(host.MininetHost(executor), 14000))

        num_fake_leaders = f + 1
        for i in range(num_fake_leaders):
            fake_leader = self._net.addHost(f'f{i}')
            self._net.addLink(fake_leader, switch)
            self._fake_leaders.append(
                host.Endpoint(host.MininetHost(fake_leader), 14000))




    def net(self) -> mininet.net.Mininet:
        return self._net

    def f(self) -> int:
        return self._f

    def rs_type(self) -> RoundSystemType:
        return self._rs_type

    def clients(self) -> List[host.Endpoint]:
        return self._clients

    def leaders(self) -> List[host.Endpoint]:
        return self._leaders

    def leader_elections(self) -> List[host.Endpoint]:
        return self._leader_elections

    def leader_heartbeats(self) -> List[host.Endpoint]:
        return self._leader_heartbeats

    def acceptors(self) -> List[host.Endpoint]:
        return self._acceptors

    def acceptor_heartbeats(self) -> List[host.Endpoint]:
        return self._acceptor_heartbeats

    def proposers(self) -> List[host.Endpoint]:
        return self._proposers

    def executors(self) -> List[host.Endpoint]:
        return self._executors

    def fake_leaders(self) -> List[host.Endpoint]:
        return self._fake_leaders


# Suite ########################################################################
class SPaxosDecoupleSuite(benchmark.Suite[Input, Output]):
    def make_net(self, args: Dict[Any, Any], input: Input) -> SPaxosDecoupleNet:
        if args['address'] is not None:
            return RemoteSPaxosDecoupleNet(
                        args['address'],
                        f=input.f,
                        rs_type=input.round_system_type,
                        num_client_procs=input.num_client_procs)
        else:
            return SingleSwitchMininet(
                        f=input.f,
                        rs_type=input.round_system_type,
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
                       net: SPaxosDecoupleNet) -> Output:
        # Write config file.
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(net.config()))
        bench.log('Config file config.pbtxt written.')

        # Launch acceptors.
        acceptor_procs = []
        for (i, acceptor) in enumerate(net.acceptors()):
            proc = bench.popen(
                host=acceptor.host,
                label=f'acceptor_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.spaxosdecouple.AcceptorMain',
                    '--index', str(i),
                    '--config', config_filename,
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
        for (i, leader) in enumerate(net.leaders()):
            proc = bench.popen(
                host=leader.host,
                label=f'leader_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.spaxosdecouple.LeaderMain',
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

        proposer_procs = []
        for (i, proposer) in enumerate(net.proposers()):
            proc = bench.popen(
                host=proposer.host,
                label=f'proposer_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.spaxosdecouple.ProposerMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--prometheus_host', proposer.host.ip(),
                    '--prometheus_port',
                        str(proposer.port + 1) if input.monitored else '-1',
                ],
            )
            proposer_procs.append(proc)
        bench.log('Proposers started.')

        # Launch executors
        executor_procs = []
        for (i, executor) in enumerate(net.executors()):
            proc = bench.popen(
                host=executor.host,
                label=f'executor_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.spaxosdecouple.ExecutorMain',
                    '--index', str(i),
                    '--state_machine', input.state_machine,
                    '--config', config_filename,
                    '--prometheus_host', executor.host.ip(),
                    '--prometheus_port',
                    str(executor.port + 1) if input.monitored else '-1',
                    '--options.waitPeriod',
                    f'{input.executor.wait_period_ms}ms',
                    '--options.waitStagger',
                    f'{input.executor.wait_stagger_ms}ms',
                ],
            )
            executor_procs.append(proc)
        bench.log('Executors started.')

        # Launch fake leaders
        fake_leader_procs = []
        for (i, fake_leader) in enumerate(net.fake_leaders()):
            proc = bench.popen(
                host=fake_leader.host,
                label=f'fake_leader_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.spaxosdecouple.FakeLeaderMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--prometheus_host', fake_leader.host.ip(),
                    '--prometheus_port',
                    str(executor.port + 1) if input.monitored else '-1',
                ],
            )
            fake_leader_procs.append(proc)
        bench.log('Fake Leaders started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                input.prometheus_scrape_interval_ms,
                {
                  'spaxos_decouple_acceptor':
                    [f'{e.host.ip()}:{e.port + 1}' for e in net.acceptors()],
                  'spaxos_decouple_leader':
                    [f'{e.host.ip()}:{e.port + 1}' for e in net.leaders()],
                  'spaxos_decouple_client':
                    [f'{e.host.ip()}:{e.port + 1}' for e in net.clients()],
                  'spaxos_decouple_proposer':
                    [f'{e.host.ip()}:{e.port + 1}' for e in net.proposers()],
                  'spaxos_decouple_executor':
                    [f'{e.host.ip()}:{e.port + 1}' for e in net.executors()],
                  'spaxos_decouple_fake_leader':
                    [f'{e.host.ip()}:{e.port + 1}' for e in net.fake_leaders()],

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
        for (i, client) in enumerate(net.clients()):
            proc = bench.popen(
                host=client.host,
                label=f'client_{i}',
                cmd = [
                    'timeout', f'{input.timeout_seconds}s',
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.spaxosdecouple.BenchmarkClientMain',
                    '--host', client.host.ip(),
                    '--port', str(client.port),
                    '--prometheus_host', client.host.ip(),
                    '--prometheus_port',
                        str(client.port + 1) if input.monitored else '-1',
                    '--config', config_filename,
                    '--options.reproposePeriod',
                        f'{input.client.repropose_period_ms}ms',
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
        for proc in leader_procs + acceptor_procs + proposer_procs + executor_procs + fake_leader_procs:
            proc.kill()
        if input.monitored:
            prometheus_server.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        # TODO(mwhittaker): Implement.
        return benchmark.parse_recorder_data(bench, client_csvs,
                drop_prefix=datetime.timedelta(seconds=0))


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
