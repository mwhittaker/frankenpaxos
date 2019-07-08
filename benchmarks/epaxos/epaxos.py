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


class ReplicaOptions(NamedTuple):
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
    execute_graph_batch_size: int = 1
    execute_graph_timer_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)


class ClientOptions(NamedTuple):
    repropose_period: datetime.timedelta = datetime.timedelta(milliseconds=100)


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

    # Benchmark parameters. ####################################################
    # The (rough) duration of the benchmark warmup.
    warmup_duration: datetime.timedelta
    # Warmup timeout.
    warmup_timeout: datetime.timedelta
    # Warmup sleep time.
    warmup_sleep: datetime.timedelta
    # The (rough) duration of the benchmark.
    duration: datetime.timedelta
    # Global timeout.
    timeout: datetime.timedelta
    # Delay between starting replicas and clients.
    client_lag: datetime.timedelta
    # Profile the code with perf.
    profiled: bool
    # Monitor the code with prometheus.
    monitored: bool
    # The interval between Prometheus scrapes. This field is only relevant if
    # monitoring is enabled.
    prometheus_scrape_interval: datetime.timedelta

    # Replica options. #########################################################
    replica_options: ReplicaOptions
    replica_log_level: str

    # Client parameters. #######################################################
    client_options: ClientOptions
    client_log_level: str
    client_num_keys: int


Output = benchmark.RecorderOutput


class EPaxosNet(object):
    def __init__(self) -> None:
        pass

    def __enter__(self) -> 'EPaxosNet':
        self.net().start()
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        self.net().stop()

    def net(self) -> Mininet:
        raise NotImplementedError()

    def clients(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def replicas(self) -> List[mininet.node.Node]:
        raise NotImplementedError()

    def config(self) -> proto_util.Message:
        raise NotImplementedError()


class SingleSwitchNet(EPaxosNet):
    def __init__(self,
                 f: int,
                 num_client_procs: int) -> None:
        self.f = f
        self._clients: List[mininet.node.Node] = []
        self._replicas: List[mininet.node.Node] = []
        self._net = Mininet()

        switch = self._net.addSwitch('s1')
        self._net.addController('c')

        for i in range(num_client_procs):
            client = self._net.addHost(f'c{i}')
            self._net.addLink(client, switch)
            self._clients.append(client)

        num_replicas = 2*f + 1
        for i in range(num_replicas):
            replica = self._net.addHost(f'r{i}')
            self._net.addLink(replica, switch)
            self._replicas.append(replica)

    def net(self) -> Mininet:
        return self._net

    def clients(self) -> List[mininet.node.Node]:
        return self._clients

    def replicas(self) -> List[mininet.node.Node]:
        return self._replicas

    def config(self) -> proto_util.Message:
        return {
            'f': self.f,
            'replicaAddress': [
                {'host': a.IP(), 'port': 9000}
                for (i, a) in enumerate(self.replicas())
            ],
        }


class EPaxosSuite(benchmark.Suite[Input, Output]):
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
        for (i, host) in enumerate(net.replicas()):
            proc = bench.popen(
                f=host.popen,
                label=f'replica_{i}',
                cmd = [
                    'java',
                    '-cp', os.path.abspath(args['jar']),
                    'frankenpaxos.epaxos.ReplicaMain',
                    '--index', str(i),
                    '--config', config_filename,
                    '--log_level', input.replica_log_level,
                    '--prometheus_host', host.IP(),
                    '--prometheus_port', '12345' if input.monitored else '-1',
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
                ],
                profile=input.profiled,
            )
            replica_procs.append(proc)
        bench.log('Replicas started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000),
                {
                  'epaxos_replica': [f'{r.IP()}:12345' for r in net.replicas()],
                  'epaxos_client': [f'{c.IP()}:12345' for c in net.clients()],
                }
            )
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                f=net.replicas()[0].popen,
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
                    'frankenpaxos.epaxos.BenchmarkClientMain',
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
        for proc in replica_procs:
            proc.terminate()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [bench.abspath(f'client_{i}_data.csv')
                       for i in range(input.num_client_procs)]
        return benchmark.parse_recorder_data(bench, client_csvs,
                drop_prefix=input.warmup_duration + input.warmup_sleep)

    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        with SingleSwitchNet(f=input.f,
                             num_client_procs=input.num_client_procs) as net:
            return self._run_benchmark(bench, args, input, net)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
