from .. import benchmark
from .. import parser_util
from .. import pd_util
from .. import prometheus
from .. import proto_util
from .. import util
from mininet.net import Mininet
from typing import Callable, Dict, List, NamedTuple
import argparse
import csv
import enum
import mininet
import os
import pandas as pd
import subprocess
import time
import tqdm
import yaml


class RoundSystemType(enum.Enum):
  CLASSIC_ROUND_ROBIN = 0
  ROUND_ZERO_FAST = 1
  MIXED_ROUND_ROBIN = 2


class ThriftySystemType:
  NOT_THRIFTY = "NotThrifty"
  RANDOM = "Random"
  CLOSEST = "Closest"


class ElectionOptions(NamedTuple):
    ping_period_ms: float = 1 * 1000
    no_ping_timeout_min_ms: float = 10 * 1000
    no_ping_timeout_max_ms: float = 12 * 1000
    not_enough_votes_timeout_min_ms: float = 10 * 1000
    not_enough_votes_timeout_max_ms: float = 12 * 1000


class HeartbeatOptions(NamedTuple):
    fail_period_ms: float = 5 * 1000
    success_period_ms: float = 10 * 1000
    num_retries: int = 3
    network_delay_alpha: float = 0.9


class AcceptorOptions(NamedTuple):
    wait_period_ms: float = 25
    wait_stagger_ms: float = 25


class LeaderOptions(NamedTuple):
    thrifty_system: str = ThriftySystemType.NOT_THRIFTY
    resend_phase1as_timer_period_ms: float = 5 * 1000
    resend_phase2as_timer_period_ms: float = 5 * 1000
    election: ElectionOptions = ElectionOptions()
    heartbeat: HeartbeatOptions = HeartbeatOptions()

class ClientOptions(NamedTuple):
    repropose_period_ms: float = 10 * 1000

class Input(NamedTuple):
    # System-wide parameters.
    net_name: str
    f: int
    num_clients: int
    num_threads_per_client: int
    round_system_type: str

    # Benchmark parameters.
    duration_seconds: float
    timeout_seconds: float
    client_lag_seconds: float
    profiled: bool
    monitored: bool
    prometheus_scrape_interval_ms: int

    # Acceptor options.
    acceptor: AcceptorOptions = AcceptorOptions()

    # Leader options.
    leader: LeaderOptions = LeaderOptions()

    # Client options.
    client: ClientOptions = ClientOptions()


class Output(NamedTuple):
    mean_latency_ms: float
    median_latency_ms: float
    p90_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float

    mean_1_second_throughput: float
    median_1_second_throughput: float
    p90_1_second_throughput: float
    p95_1_second_throughput: float
    p99_1_second_throughput: float

    mean_2_second_throughput: float
    median_2_second_throughput: float
    p90_2_second_throughput: float
    p95_2_second_throughput: float
    p99_2_second_throughput: float

    mean_5_second_throughput: float
    median_5_second_throughput: float
    p90_5_second_throughput: float
    p95_5_second_throughput: float
    p99_5_second_throughput: float


class FastMultiPaxosNet(object):
    def __init__(self) -> None:
        pass

    def __enter__(self) -> 'FastMultiPaxosNet':
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

    def acceptors(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def config(self) -> proto_util.Message:
        raise NotImplementedError()


class SingleSwitchNet(FastMultiPaxosNet):
    def __init__(self,
                 f: int,
                 num_clients: int,
                 rs_type: RoundSystemType) -> None:
        self.f = f
        self.rs_type = rs_type
        self._clients: List[mininet.node.Node] = []
        self._leaders: List[mininet.node.Node] = []
        self._acceptors: List[mininet.node.Node] = []
        self._net = Mininet()

        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        for i in range(num_clients):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(client)

        num_leaders = f + 1
        for i in range(num_leaders):
            leader = self._net.addHost(f'l{i}')
            self._net.addLink(leader, switch)
            self._leaders.append(leader)

        num_acceptors = 2*f + 1
        for i in range(num_acceptors):
            acceptor = self._net.addHost(f'a{i}')
            self._net.addLink(acceptor, switch)
            self._acceptors.append(acceptor)

    def net(self) -> Mininet:
        return self._net

    def clients(self) -> List[mininet.node.Node]:
        return self._clients

    def leaders(self) -> List[mininet.node.Node]:
        return self._leaders

    def acceptors(self) -> List[mininet.node.Node]:
        return self._acceptors

    def config(self) -> proto_util.Message:
        return {
            'f': self.f,
            'leaderAddress': [
                {'host': l.IP(), 'port': 9000}
                for (i, l) in enumerate(self.leaders())
            ],
            'leaderElectionAddress': [
                {'host': l.IP(), 'port': 9001}
                for (i, l) in enumerate(self.leaders())
            ],
            'leaderHeartbeatAddress': [
                {'host': l.IP(), 'port': 9002}
                for (i, l) in enumerate(self.leaders())
            ],
            'acceptorAddress': [
                {'host': a.IP(), 'port': 10000}
                for (i, a) in enumerate(self.acceptors())
            ],
            'acceptorHeartbeatAddress': [
                {'host': a.IP(), 'port': 10001}
                for (i, a) in enumerate(self.acceptors())
            ],
            'roundSystemType': self.rs_type
        }


def run_benchmark(bench: benchmark.BenchmarkDirectory,
                  args: argparse.Namespace,
                  input: Input,
                  net: FastMultiPaxosNet) -> Output:
    # Write config file.
    config_filename = bench.abspath('config.pbtxt')
    bench.write_string(config_filename,
                       proto_util.message_to_pbtext(net.config()))
    bench.log('Config file config.pbtxt written.')

    # Launch acceptors.
    acceptor_procs = []
    for (i, host) in enumerate(net.acceptors()):
        proc = bench.popen(
            f=host.popen,
            label=f'acceptor_{i}',
            cmd = [
                'java',
                '-cp', os.path.abspath(args.jar),
                'frankenpaxos.fastmultipaxos.AcceptorMain',
                # Basic flags.
                '--index', str(i),
                '--config', config_filename,
                # Options.
                '--options.waitPeriod', f'{input.acceptor.wait_period_ms}ms',
                '--options.waitStagger', f'{input.acceptor.wait_stagger_ms}ms',
            ],
            profile=args.profile,
        )
        acceptor_procs.append(proc)
    bench.log('Acceptors started.')

    # Launch leaders.
    leader_procs = []
    for (i, host) in enumerate(net.leaders()):
        proc = bench.popen(
            f=host.popen,
            label=f'leader_{i}',
            cmd = [
                'java',
                '-cp', os.path.abspath(args.jar),
                'frankenpaxos.fastmultipaxos.LeaderMain',
                # Basic flags.
                '--index', str(i),
                '--config', config_filename,
                # Monitoring.
                '--prometheus_host', host.IP(),
                '--prometheus_port', '12345',
                # Options.
                '--options.thriftySystem',
                    input.leader.thrifty_system,
                '--options.resendPhase1asTimerPeriod',
                    f'{input.leader.resend_phase1as_timer_period_ms}ms',
                '--options.resendPhase2asTimerPeriod',
                    f'{input.leader.resend_phase2as_timer_period_ms}ms',
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
            profile=args.profile,
        )
        leader_procs.append(proc)
    bench.log('Leaders started.')

    # Launch Prometheus.
    if input.monitored:
        prometheus_config = prometheus.prometheus_config(
            input.prometheus_scrape_interval_ms,
            {
              'fast_multipaxos_leader': [f'{l.IP()}:12345' for l in net.leaders()],
              'fast_multipaxos_client': [f'{c.IP()}:12345' for c in net.clients()],
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

    # Wait a bit so that a stable leader can elect itself. If we start
    # clients too soon, they may not talk to a stable leader.
    time.sleep(input.client_lag_seconds)
    bench.log('Client lag ended.')

    # Launch clients.
    client_procs = []
    for (i, host) in enumerate(net.clients()):
        proc = bench.popen(
            f=host.popen,
            label=f'client_{i}',
            cmd = [
                'timeout', f'{input.timeout_seconds}s',
                'java',
                '-cp', os.path.abspath(args.jar),
                'frankenpaxos.fastmultipaxos.BenchmarkClientMain',
                '--host', host.IP(),
                '--port', "11000",
                '--prometheus_host', host.IP(),
                '--prometheus_port', "12345",
                '--config', config_filename,
                '--options.reproposePeriod',
                    f'{input.client.repropose_period_ms}ms',
                '--duration', f'{input.duration_seconds}s',
                '--num_threads', str(input.num_threads_per_client),
                '--output_file_prefix', bench.abspath(f'client_{i}'),
            ]
        )
        client_procs.append(proc)
    bench.log('Clients started.')

    # Wait for clients to finish and then terminate everything.
    for proc in client_procs:
        proc.wait()
    for proc in leader_procs + acceptor_procs:
        proc.terminate()
    if input.monitored:
        prometheus_server.terminate()
    bench.log('Clients finished and processes terminated.')

    # Every client thread j on client i writes results to `client_i_j.csv`.
    # We concatenate these results into a single CSV file.
    client_csvs = [bench.abspath(f'client_{i}_{j}.csv')
                   for i in range(input.num_clients)
                   for j in range(input.num_threads_per_client)]
    df = pd_util.read_csvs(client_csvs, parse_dates=['start', 'stop'])
    bench.log('Data read.')
    df = df.set_index('start')
    bench.log('Data index set.')
    df = df.sort_index(0)
    bench.log('Data index sorted.')
    df.to_csv(bench.abspath('data.csv'))
    bench.log('Data written.')

    # Since we concatenate and save the file, we can throw away the originals.
    for client_csv in client_csvs:
        os.remove(client_csv)

    # We also compress the output data since it can get big.
    subprocess.call(['gzip', bench.abspath('data.csv')])
    bench.log('Data compressed.')

    latency_ms = df['latency_nanos'] / 1e6
    throughput_1s = pd_util.throughput(df, 1000)
    throughput_2s = pd_util.throughput(df, 2000)
    throughput_5s = pd_util.throughput(df, 5000)
    return Output(
        mean_latency_ms = latency_ms.mean(),
        median_latency_ms = latency_ms.median(),
        p90_latency_ms = latency_ms.quantile(.90),
        p95_latency_ms = latency_ms.quantile(.95),
        p99_latency_ms = latency_ms.quantile(.99),

        mean_1_second_throughput = throughput_1s.mean(),
        median_1_second_throughput = throughput_1s.median(),
        p90_1_second_throughput = throughput_1s.quantile(.90),
        p95_1_second_throughput = throughput_1s.quantile(.95),
        p99_1_second_throughput = throughput_1s.quantile(.99),

        mean_2_second_throughput = throughput_2s.mean(),
        median_2_second_throughput = throughput_2s.median(),
        p90_2_second_throughput = throughput_2s.quantile(.90),
        p95_2_second_throughput = throughput_2s.quantile(.95),
        p99_2_second_throughput = throughput_2s.quantile(.99),

        mean_5_second_throughput = throughput_5s.mean(),
        median_5_second_throughput = throughput_5s.median(),
        p90_5_second_throughput = throughput_5s.quantile(.90),
        p95_5_second_throughput = throughput_5s.quantile(.95),
        p99_5_second_throughput = throughput_5s.quantile(.99),
    )


def run_suite(args: argparse.Namespace,
              inputs: List[Input],
              make_net: Callable[[Input], FastMultiPaxosNet]) -> None:
    assert len(inputs) > 0, inputs

    with benchmark.SuiteDirectory(
            args.suite_directory, 'fast_multipaxos') as suite:
        print(f'Running benchmark suite in {suite.path}.')
        suite.write_dict('args.json', vars(args))
        suite.write_string('inputs.txt', '\n'.join(str(i) for i in inputs))

        results_file = suite.create_file('results.csv')
        results_writer = csv.writer(results_file)
        results_writer.writerow(util.flatten_tuple_fields(inputs[0]) +
                                list(Output._fields))
        results_file.flush()

        for input in tqdm.tqdm(inputs):
            with suite.benchmark_directory() as bench:
                with make_net(input) as net:
                    bench.write_string('input.txt', str(input))
                    bench.write_dict('input.json', input._asdict())
                    output = run_benchmark(bench, args, input, net)
                    row = util.flatten_tuple(input) + list(output)
                    results_writer.writerow([str(x) for x in row])
                    results_file.flush()


def _main(args) -> None:
    inputs = [
        Input(
            net_name='SingleSwitchNet',
            f=1,
            num_clients=num_clients,
            num_threads_per_client=1,
            round_system_type=RoundSystemType.CLASSIC_ROUND_ROBIN.name,

            duration_seconds=10,
            timeout_seconds=120,
            client_lag_seconds=3,
            profiled=args.profile,
            monitored=args.monitor,
            prometheus_scrape_interval_ms=200,

        )
        for num_clients in [1, 2]
    ] * 2

    def make_net(input) -> FastMultiPaxosNet:
        return SingleSwitchNet(
            f=input.f,
            num_clients=input.num_clients,
            rs_type = RoundSystemType[input.round_system_type]
        )

    run_suite(args, inputs, make_net)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()


if __name__ == '__main__':
    _main(get_parser().parse_args())
