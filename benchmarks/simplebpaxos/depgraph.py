from .simplebpaxos import *


def main(args) -> None:
    class DepGraphSimpleBPaxosSuite(SimpleBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = num_clients_per_proc,
                    num_leaders = 5,
                    warmup_duration = datetime.timedelta(seconds=5),
                    warmup_timeout = datetime.timedelta(seconds=10),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=20),
                    timeout = datetime.timedelta(seconds=30),
                    client_lag = datetime.timedelta(seconds=2),
                    state_machine = 'KeyValueStore',
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys = 1000000,
                        size_mean = 1,
                        size_std = 0,
                    ),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    leader_options = LeaderOptions(
                        resend_dependency_requests_timer_period = \
                            datetime.timedelta(seconds=60)
                    ),
                    leader_log_level = args.log_level,
                    proposer_options = ProposerOptions(
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
                        unsafe_skip_graph_execution =
                            unsafe_skip_graph_execution,
                        execute_graph_batch_size = 100,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1)
                    ),
                    replica_log_level = args.log_level,
                    replica_dependency_graph = replica_dependency_graph,
                    garbage_collector_options = GarbageCollectorOptions(),
                    garbage_collector_log_level = args.log_level,
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(seconds=60),
                    ),
                    client_log_level = args.log_level,
                )
                for unsafe_skip_graph_execution in [False, True]
                for replica_dependency_graph in (
                    ['Jgrapht', 'Tarjan', 'IncrementalTarjan']
                    if not unsafe_skip_graph_execution else ['Tarjan'])
                for (num_client_procs, num_clients_per_proc) in
                    [(1, 100), (6, 100), (6, 500)]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'skip_graph_execution':
                    input.replica_options.unsafe_skip_graph_execution,
                'dep_graph': input.replica_dependency_graph,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = DepGraphSimpleBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'simplebpaxos_dep_graph') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
