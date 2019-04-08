from . import prometheus
from . import proto_util
from . import util
from . import util
from .benchmark import BenchmarkDirectory, SuiteDirectory
from contextlib import ExitStack
from enum import Enum
from mininet.net import Mininet
from mininet.node import Node
from subprocess import Popen
from tqdm import tqdm
from typing import Callable, Dict, List, NamedTuple
import argparse
import csv
import os
import pandas as pd
import subprocess
import time
import yaml


class RoundSystemType(Enum):
  CLASSIC_ROUND_ROBIN = 0
  ROUND_ZERO_FAST = 1
  MIXED_ROUND_ROBIN = 2


class Input(NamedTuple):
    # System-wide parameters.
    net_name: str
    f: int
    num_clients: int
    num_threads_per_client: int
    round_system_type: str
    profiled: bool
    monitored: bool
    prometheus_scrape_interval_ms: int

    # Benchmark parameters.
    duration_seconds: float
    client_lag_seconds: float

    # Client parameters.
    client_repropose_period_seconds: float


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

    def clients(self) -> List[Node]:
        raise NotImplementedError()

    def leaders(self) -> List[Node]:
        raise NotImplementedError()

    def acceptors(self) -> List[Node]:
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
        self._clients: List[Node] = []
        self._leaders: List[Node] = []
        self._acceptors: List[Node] = []
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

    def clients(self) -> List[Node]:
        return self._clients

    def leaders(self) -> List[Node]:
        return self._leaders

    def acceptors(self) -> List[Node]:
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


def run_benchmark(bench: BenchmarkDirectory,
                  args: argparse.Namespace,
                  input: Input,
                  net: FastMultiPaxosNet) -> Output:
    # Write config file.
    config_filename = bench.abspath('config.pbtxt')
    bench.write_string(config_filename,
                       proto_util.message_to_pbtext(net.config()))

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
                '--index', str(i),
                '--config', config_filename,
            ],
            profile=args.profile,
        )
        acceptor_procs.append(proc)

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
                '--index', str(i),
                '--config', config_filename,
                '--prometheus_host', host.IP(),
                '--prometheus_port', '12345',
            ],
            profile=args.profile,
        )
        leader_procs.append(proc)

    # Launch Prometheus.
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

    # Wait a bit so that a stable leader can elect itself. If we start
    # clients too soon, they may not talk to a stable leader.
    time.sleep(input.client_lag_seconds)

    # Launch clients.
    client_procs = []
    for (i, host) in enumerate(net.clients()):
        proc = bench.popen(
            f=host.popen,
            label=f'client_{i}',
            cmd = [
                'java',
                '-cp', os.path.abspath(args.jar),
                'frankenpaxos.fastmultipaxos.BenchmarkClientMain',
                '--host', host.IP(),
                '--port', "11000",
                '--prometheus_host', host.IP(),
                '--prometheus_port', "12345",
                '--config', config_filename,
                '--repropose_period',
                    f'{input.client_repropose_period_seconds}s',
                '--duration', f'{input.duration_seconds}s',
                '--num_threads', str(input.num_threads_per_client),
                '--output_file_prefix', bench.abspath(f'client_{i}'),
            ]
        )
        client_procs.append(proc)

    # Wait for clients to finish and then terminate everything.
    for proc in client_procs:
        proc.wait()
    for proc in leader_procs + acceptor_procs + [prometheus_server]:
        proc.terminate()

    # Every client thread j on client i writes results to `client_i_j.csv`.
    # We concatenate these results into a single CSV file.
    client_csvs = [bench.abspath(f'client_{i}_{j}.csv')
                   for i in range(input.num_clients)
                   for j in range(input.num_threads_per_client)]
    df = (util.read_csvs(client_csvs, parse_dates=['start', 'stop'])
              .set_index('start')
              .sort_index(0))
    df.to_csv(bench.abspath('data.csv'))

    # Since we concatenate and save the file, we can throw away the originals.
    for client_csv in client_csvs:
        os.remove(client_csv)

    # We also compress the output data since it can get big.
    subprocess.call(['gzip', bench.abspath('data.csv')])

    # Scrape data from Prometheus.
    pq = prometheus.PrometheusQueryer(
        tsdb_path=bench.abspath('prometheus_data'),
        popen=lambda c: bench.popen(label='prometheus_querier', cmd=c)
    )
    p_df = pq.query(
        '{__name__=~"fast_multipaxos_.*", job=~"fast_multipaxos_.*"}[1y]')
    def _rename(old_column) -> str:
        address_to_instance_name = {
            **{f'{host.IP()}:12345': f'leader_{i}'
               for (i, host) in enumerate(net.leaders())},
            **{f'{host.IP()}:12345': f'client_{i}'
               for (i, host) in enumerate(net.clients())},
        }
        label = dict(old_column)
        instance_name = address_to_instance_name[label["instance"]]
        if "type" in label:
            return f'{label["__name__"]}_{label["type"]}_{instance_name}'
        else:
            return f'{label["__name__"]}_{instance_name}'
    p_df = p_df.rename(columns=_rename)
    p_df.to_csv(bench.abspath('prometheus_data.csv'))

    latency_ms = df['latency_nanos'] / 1e6
    throughput_1s = util.throughput(df, 1000)
    throughput_2s = util.throughput(df, 2000)
    throughput_5s = util.throughput(df, 5000)
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
    with SuiteDirectory(args.suite_directory, 'fast_multipaxos') as suite:
        print(f'Running benchmark suite in {suite.path}.')
        suite.write_dict('args.json', vars(args))
        suite.write_string('inputs.txt', '\n'.join(str(i) for i in inputs))

        results_file = suite.create_file('results.csv')
        results_writer = csv.writer(results_file)
        results_writer.writerow(Input._fields + Output._fields)

        for input in tqdm(inputs):
            with suite.benchmark_directory() as bench:
                with make_net(input) as net:
                    bench.write_string('input.txt', str(input))
                    bench.write_dict('input.json', input._asdict())
                    output = run_benchmark(bench, args, input, net)
                    row = [str(x) for x in list(input) + list(output)]
                    results_writer.writerow(row)
                    results_file.flush()


def _main(args) -> None:
    inputs = [
        Input(
            net_name='SingleSwitchNet',
            f=1,
            num_clients=num_clients,
            num_threads_per_client=1,
            round_system_type=RoundSystemType.CLASSIC_ROUND_ROBIN.name,
            profiled=args.profile,
            monitored=args.monitor,
            prometheus_scrape_interval_ms=200,
            duration_seconds=10,
            client_lag_seconds=3,
            client_repropose_period_seconds=10,
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
    return util.get_parser()


if __name__ == '__main__':
    _main(get_parser().parse_args())
