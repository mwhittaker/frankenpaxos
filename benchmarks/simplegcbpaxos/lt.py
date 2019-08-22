from .simplegcbpaxos import *


def main(args) -> None:
    class LatencyThroughputSimpleGcBPaxosSuite(SimpleGcBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = f,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = num_clients_per_proc,
                    num_leaders = num_leaders,
                    warmup_duration = datetime.timedelta(seconds=3),
                    warmup_timeout = datetime.timedelta(seconds=5),
                    warmup_sleep = datetime.timedelta(seconds=2),
                    duration = datetime.timedelta(seconds=10),
                    timeout = datetime.timedelta(seconds=15),
                    client_lag = datetime.timedelta(seconds=2),
                    state_machine = 'KeyValueStore',
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys=1000000,
                        size_mean=1,
                        size_std=0,
                    ),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    leader_options = LeaderOptions(
                        thrifty_system = thrifty_system,
                        resend_dependency_requests_timer_period = \
                            datetime.timedelta(seconds=60)
                    ),
                    leader_log_level = args.log_level,
                    proposer_options = ProposerOptions(
                        thrifty_system = thrifty_system,
                        resend_phase1as_timer_period = \
                            datetime.timedelta(seconds=60),
                        resend_phase2as_timer_period = \
                            datetime.timedelta(seconds=60),
                    ),
                    proposer_log_level = args.log_level,
                    dep_service_node_options = DepServiceNodeOptions(),
                    dep_service_node_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    replica_options = ReplicaOptions(
                        recover_vertex_timer_min_period = \
                            datetime.timedelta(seconds=60),
                        recover_vertex_timer_max_period = \
                            datetime.timedelta(seconds=120),
                        execute_graph_batch_size = 100,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1)
                    ),
                    replica_log_level = args.log_level,
                    replica_dependency_graph = dependency_graph,
                    garbage_collector_options = GarbageCollectorOptions(),
                    garbage_collector_log_level = args.log_level,
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(seconds=60),
                    ),
                    client_log_level = args.log_level,
                )
                for (f, num_leaders, dependency_graph, thrifty_system) in [
                    # Sweep the number of leaders.
                    (1, 2, 'Tarjan', 'Random'),
                    (1, 3, 'Tarjan', 'Random'),
                    (1, 4, 'Tarjan', 'Random'),
                    (1, 5, 'Tarjan', 'Random'),
                    (1, 6, 'Tarjan', 'Random'),
                    (1, 7, 'Tarjan', 'Random'),
                    # Sweep the dependency graph.
                    (1, 6, 'IncrementalTarjan', 'Random'),
                    (1, 6, 'Jgrapht', 'Random'),
                    # Sweep the thriftiness.
                    (1, 6, 'Tarjan', 'NotThrifty'),
                    # Sweep f.
                    (2, 6, 'Tarjan', 'Random'),
                ]
                for (num_client_procs, num_clients_per_proc) in [
                    (2, 100),
                    (4, 100),
                    (6, 100),
                    (6, 500),
                    (6, 1000),
                    (6, 2000),
                ]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_leaders': input.num_leaders,
                'dep_graph': input.replica_dependency_graph,
                'thrifty_system': input.leader_options.thrifty_system,
                'num_clients':
                    (input.num_client_procs, input.num_clients_per_proc),
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = LatencyThroughputSimpleGcBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'simplegcbpaxos_latency_throughput') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
