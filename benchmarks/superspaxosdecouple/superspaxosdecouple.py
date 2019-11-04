from .. import benchmark
from .. import cluster
from .. import host
from .. import parser_util
from .. import pd_util
from .. import perf_util
from .. import proc
from .. import prometheus
from .. import proto_util
from .. import util
from .. import workload
from ..spaxosdecouple import spaxosdecouple
from ..workload import Workload
from typing import Any, Callable, Collection, Dict, List, NamedTuple, Optional
import argparse
import csv
import datetime
import enum
import enum
import itertools
import os
import pandas as pd
import paramiko
import subprocess
import time
import tqdm
import yaml


# Suite ########################################################################
class SuperSPaxosDecoupleSuite(benchmark.Suite[spaxosdecouple.Input, spaxosdecouple.Output]
                          ):
    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: spaxosdecouple.Input) -> spaxosdecouple.Output:
        net = spaxosdecouple.SPaxosDecoupleNet(args['cluster'], args['identity_file'],
                                       input)
        return self._run_benchmark(bench, args, input, net)

    def _run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any], input: spaxosdecouple.Input,
                       net: spaxosdecouple.SPaxosDecoupleNet) -> spaxosdecouple.Output:
        # Write config file.
        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
        bench.log('Config file config.pbtxt written.')

        def java(heap_size: str) -> List[str]:
            cmd = ['java', f'-Xms{heap_size}', f'-Xmx{heap_size}']
            if input.monitored:
                cmd += [
                    '-verbose:gc',
                    '-XX:-PrintGC',
                    '-XX:+PrintHeapAtGC',
                    '-XX:+PrintGCDetails',
                    '-XX:+PrintGCTimeStamps',
                    '-XX:+PrintGCDateStamps',
                ]
            return cmd

        # Launch super nodes.
        assert (len(net.placement().batchers) == 0 or
                len(net.placement().leaders) == len(net.placement().batchers))
        assert len(net.placement().leaders) == len(
            net.placement().proxy_leaders)
        assert len(net.placement().acceptors) == 1
        assert len(net.placement().leaders) == len(net.placement().acceptors[0])
        assert len(net.placement().leaders) == len(net.placement().replicas)
        assert len(net.placement().leaders) == len(
            net.placement().proxy_replicas)

        super_node_procs: List[proc.Proc] = []
        for (i, leader) in enumerate(net.placement().leaders):
            cmd = java(input.leader_jvm_heap_size) + [
                '-cp',
                os.path.abspath(args['jar']),
                'frankenpaxos.spaxosdecouple.SuperNodeMain',
                '--index',
                str(i),
                '--config',
                config_filename,
                '--log_level',
                input.leader_log_level,
                '--prometheus_host',
                leader.host.ip(),
                '--prometheus_port',
                str(leader.port + 1) if input.monitored else '-1',

                # Leader options.
                '--leader.resendPhase1asPeriod',
                '{}s'.format(input.leader_options.resend_phase1as_period.
                             total_seconds()),
                '--leader.flushPhase2asEveryN',
                str(input.leader_options.flush_phase2as_every_n),
                '--leader.election.pingPeriod',
                '{}s'.format(input.leader_options.election_options.ping_period.
                             total_seconds()),
                '--leader.election.noPingTimeoutMin',
                '{}s'.format(input.leader_options.election_options.
                             no_ping_timeout_min.total_seconds()),
                '--leader.election.noPingTimeoutMax',
                '{}s'.format(input.leader_options.election_options.
                             no_ping_timeout_max.total_seconds()),

                # ProxyLeader options.
                '--proxy_leader.flushPhase2asEveryN',
                str(input.proxy_leader_options.flush_phase2as_every_n),
                '--proxy_leader.flushValueChosensEveryN',
                str(input.proxy_leader_options.flush_value_chosens_every_n),

                # Proposer options
                '--proposer.flushForwardsEveryN',
                str(input.proposer_options.flush_forwards_every_n),
                '--proposer.flushClientRequestsEveryN',
                str(input.proposer_options.flush_client_requests_every_n),

                # Disseminator Options
                '--disseminator.flushChosensEveryN',
                str(input.disseminator_options.flush_chosens_every_n),
                '--disseminator.flushAcknowledgeEveryN',
                str(input.disseminator_options.flush_acknowledge_every_n),

                # Acceptor options.

                # Replica options.
                '--replica.logGrowSize',
                str(input.replica_options.log_grow_size),
                '--replica.unsafeDontUseClientTable',
                str(input.replica_options.unsafe_dont_use_client_table),
                '--replica.sendChosenWatermarkEveryNEntries',
                str(input.replica_options.send_chosen_watermark_every_n_entries
                   ),
                '--replica.recoverLogEntryMinPeriod',
                '{}s'.format(input.replica_options.recover_log_entry_min_period.
                             total_seconds()),
                '--replica.recoverLogEntryMaxPeriod',
                '{}s'.format(input.replica_options.recover_log_entry_max_period.
                             total_seconds()),
                '--replica.unsafeDontRecover',
                str(input.replica_options.unsafe_dont_recover),

                # ProxyReplica options.
                '--proxy_replica.flushEveryN',
                str(input.proxy_replica_options.flush_every_n),
            ]
            if len(net.placement().batchers) != 0:
                cmd += [
                    # Batcher options.
                    '--batcher.batchSize',
                    str(input.batcher_options.batch_size),
                ]

            p = bench.popen(host=leader.host, label=f'super_node_{i}', cmd=cmd)
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, leader.host, p,
                                           f'super_node_{i}')
            super_node_procs.append(p)
        bench.log('SuperNodes started.')

        # Launch Prometheus.
        # TODO(mwhittaker): Is this right?
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'spaxosdecouple_client': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().clients
                    ],
                    'spaxosdecouple_batcher': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().batchers
                    ],
                    'spaxosdecouple_proposer': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().proposers
                    ],
                    'spaxosdecouple_disseminator': [
                        f'{e.host.ip()}:{e.port+1}'
                        for group in net.placement().disseminators
                        for e in group
                    ],
                    'spaxosdecouple_leader': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().leaders
                    ],
                    'spaxosdecouple_proxy_leader': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().proxy_leaders
                    ],
                    'spaxosdecouple_acceptor': [
                        f'{e.host.ip()}:{e.port+1}'
                        for group in net.placement().acceptors
                        for e in group
                    ],
                    'spaxosdecouple_replica': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().replicas
                    ],
                    'spaxosdecouple_proxy_replica': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().proxy_replicas
                    ],
                })
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.placement().clients[0].host,
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

        client_procs: List[proc.Proc] = []
        for (i, client) in enumerate(net.placement().clients):
            p = bench.popen(
                host=client.host,
                label=f'client_{i}',
                # TODO(mwhittaker): For now, we don't run clients with large
                # heaps and verbose garbage collection because they are all
                # colocated on one machine.
                cmd=[
                    'java',
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.spaxosdecouple.ClientMain',
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
                    '--warmup_duration',
                    f'{input.warmup_duration.total_seconds()}s',
                    '--warmup_timeout',
                    f'{input.warmup_timeout.total_seconds()}s',
                    '--warmup_sleep',
                    f'{input.warmup_sleep.total_seconds()}s',
                    '--num_warmup_clients',
                    f'{input.num_warmup_clients_per_proc}',
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
                    '--options.resendClientRequestPeriod',
                    '{}s'.format(input.client_options.
                                 resend_client_request_period.total_seconds()),
                ])
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, client.host, p, f'client_{i}')
            client_procs.append(p)
        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for clients to finish and then terminate leaders and acceptors.
        for p in client_procs:
            p.wait()
        for p in super_node_procs:
            p.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [
            bench.abspath(f'client_{i}_data.csv')
            for i in range(input.num_client_procs)
        ]
        return benchmark.parse_recorder_data(
            bench, client_csvs, drop_prefix=datetime.timedelta(seconds=0))


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
