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
from ..simplebpaxos import simplebpaxos
from ..workload import Workload
from typing import Any, Callable, Collection, Dict, List, NamedTuple, Optional
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
import tqdm
import yaml


# Suite ########################################################################
class SuperBPaxosSuite(benchmark.Suite[simplebpaxos.Input, simplebpaxos.Output]
                      ):
    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: simplebpaxos.Input) -> simplebpaxos.Output:
        net = simplebpaxos.SimpleBPaxosNet(args['cluster'],
                                           args['identity_file'], input)
        return self._run_benchmark(bench, args, input, net)

    def _run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                       args: Dict[Any, Any], input: simplebpaxos.Input,
                       net: simplebpaxos.SimpleBPaxosNet
                      ) -> simplebpaxos.Output:
        # Write config file.
        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
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

        # Launch super nodes!
        assert (len(net.placement().leaders) == len(
            net.placement().dep_service_nodes))
        assert len(net.placement().leaders) == len(net.placement().proposers)
        assert len(net.placement().leaders) == len(net.placement().acceptors)
        assert len(net.placement().leaders) == len(net.placement().replicas)
        endhosts = [
            net.placement().leaders,
            net.placement().dep_service_nodes,
            net.placement().proposers,
            net.placement().acceptors,
            net.placement().replicas,
        ]

        super_node_procs: List[proc.Proc] = []
        for (i, nodes) in enumerate(zip(*endhosts)):
            (leader, depnode, proposer, acceptor, replica) = nodes
            p = bench.popen(
                host=leader.host,
                label=f'super_node_{i}',
                cmd=java + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.simplebpaxos.SuperNodeMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.leader_log_level,
                    '--state_machine',
                    input.state_machine,
                    '--prometheus_host',
                    leader.host.ip(),
                    '--prometheus_port',
                    str(leader.port + 1) if input.monitored else '-1',
                    # Leader options.
                    '--leader.thriftySystem',
                    input.leader_options.thrifty_system,
                    '--leader.resendDependencyRequestsTimerPeriod',
                    '{}s'.format(input.leader_options.
                                 resend_dependency_requests_timer_period.
                                 total_seconds()),
                    # Dependency service options.
                    '--depnode.topKDependencies',
                    str(input.dep_service_node_options.top_k_dependencies),
                    '--depnode.unsafeReturnNoDependencies',
                    str(input.dep_service_node_options.
                        unsafe_return_no_dependencies),
                    # Proposer options.
                    '--proposer.thriftySystem',
                    input.proposer_options.thrifty_system,
                    '--proposer.resendPhase1asTimerPeriod',
                    '{}s'.format(input.proposer_options.
                                 resend_phase1as_timer_period.total_seconds()),
                    '--proposer.resendPhase2asTimerPeriod',
                    '{}s'.format(input.proposer_options.
                                 resend_phase2as_timer_period.total_seconds()),
                    # Acceptor options.
                    # Replica options.
                    '--replica.recoverVertexTimerMinPeriod',
                    '{}s'.format(
                        input.replica_options.recover_vertex_timer_min_period.
                        total_seconds()),
                    '--replica.recoverVertexTimerMaxPeriod',
                    '{}s'.format(
                        input.replica_options.recover_vertex_timer_max_period.
                        total_seconds()),
                    '--replica.unsafeSkipGraphExecution',
                    "true" if input.replica_options.unsafe_skip_graph_execution
                    else "false",
                    '--replica.executeGraphBatchSize',
                    str(input.replica_options.execute_graph_batch_size),
                    '--replica.executeGraphTimerPeriod',
                    '{}s'.format(input.replica_options.
                                 execute_graph_timer_period.total_seconds()),
                    '--replica.numBlockers',
                    str(input.replica_options.num_blockers),
                    '--zigzag.verticesGrowSize',
                    str(input.replica_zigzag_options.vertices_grow_size),
                    '--zigzag.garbageCollectEveryNCommands',
                    str(input.replica_zigzag_options.
                        garbage_collect_every_n_commands),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, leader.host, p,
                                           f'super_node_{i}')
            super_node_procs.append(p)
        bench.log('SuperNodes started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'bpaxos_leader': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().leaders
                    ],
                    'bpaxos_proposer': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().proposers
                    ],
                    'bpaxos_acceptor': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().acceptors
                    ],
                    'bpaxos_client': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().clients
                    ],
                    'bpaxos_dep_service_node': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().dep_service_nodes
                    ],
                    'bpaxos_replica': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().replicas
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
                    'frankenpaxos.simplebpaxos.BenchmarkClientMain',
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
                    '--options.reproposePeriod',
                    '{}s'.format(
                        input.client_options.repropose_period.total_seconds()),
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
