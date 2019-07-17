from .simplebpaxos import *


def main(args) -> None:
    class LatencyThroughputSimpleBPaxosSuite(SimpleBPaxosSuite):
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
                        execute_graph_batch_size = execute_graph_batch_size,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1)
                    ),
                    replica_log_level = args.log_level,
                    replica_dependency_graph = 'Tarjan',
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(seconds=60),
                    ),
                    client_log_level = args.log_level,
                )
                for f in [1, 2]
                for num_leaders in
                    [x for x in [2, 7] if f == 1] +
                    [x for x in [3, 10] if f == 2]
                for execute_graph_batch_size in [1, 10, 100, 1000]
                for (num_client_procs, num_clients_per_proc) in
                    [(1, 1000),  # 1000
                     (3, 1000),  # 3000
                     (5, 1000),  # 5000
                     (5, 2000),  # 10,000
                     (5, 6000),  # 30,000
                     (5, 10000)] # 50,000
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_leaders': input.num_leaders,
                'execute_graph_batch_size':
                    input.replica_options.execute_graph_batch_size,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = LatencyThroughputSimpleBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'simplebpaxos_latency_throughput') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
