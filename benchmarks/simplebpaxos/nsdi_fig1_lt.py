from .simplebpaxos import *


def main(args) -> None:
    class NsdiFig1LtSimpleBPaxosSuite(SimpleBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = f,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 50,
                    num_clients_per_proc = num_clients_per_proc,
                    num_leaders = num_leaders,
                    warmup_duration = datetime.timedelta(seconds=5),
                    warmup_timeout = datetime.timedelta(seconds=10),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=15),
                    timeout = datetime.timedelta(seconds=20),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    workload = load,
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    leader_options = LeaderOptions(
                        thrifty_system = 'NotThrifty',
                        resend_dependency_requests_timer_period = \
                            datetime.timedelta(seconds=600)
                    ),
                    leader_log_level = args.log_level,
                    proposer_options = ProposerOptions(
                        thrifty_system = 'Random',
                        resend_phase1as_timer_period = \
                            datetime.timedelta(seconds=600),
                        resend_phase2as_timer_period = \
                            datetime.timedelta(seconds=600),
                    ),
                    proposer_log_level = args.log_level,
                    dep_service_node_options = DepServiceNodeOptions(
                        top_k_dependencies = 1,
                    ),
                    dep_service_node_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    replica_options = ReplicaOptions(
                        recover_vertex_timer_min_period = \
                            datetime.timedelta(seconds=600),
                        recover_vertex_timer_max_period = \
                            datetime.timedelta(seconds=1200),
                        execute_graph_batch_size = execute_graph_batch_size,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1),
                        num_blockers = 1,
                    ),
                    replica_zigzag_options = ZigzagOptions(
                        vertices_grow_size = 20000,
                        garbage_collect_every_n_commands = 20000,
                    ),
                    replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(seconds=600),
                    ),
                    client_log_level = args.log_level,
                )
                # for f in [1, 2, 3]
                for f in [1, 2]
                for conflict_rate in [0.0, 0.02, 0.25, 0.50]
                for load in [
                    workload.BernoulliSingleKeyWorkload(
                        conflict_rate = conflict_rate,
                        size_mean = 8,
                        size_std = 0,
                    )
                ]
                for num_leaders in (
                    [5] if f == 1 else
                    [10] if f == 2 else
                    [15] if f == 3 else
                    []
                )
                for (num_client_procs, num_clients_per_proc) in
                    [
                        (1, 1),
                        (3, 10),
                        (6, 10),
                        (6, 100),
                        (6, 1000),
                    ]
                for execute_graph_batch_size in (
                    [1] if num_client_procs * num_clients_per_proc == 1 else
                    [100] if num_client_procs * num_clients_per_proc > 100 else
                    [int(num_client_procs * num_clients_per_proc / 2)]
                )
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'workload': input.workload,
                'num_leaders': input.num_leaders,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'execute_graph_batch_size':
                    input.replica_options.execute_graph_batch_size,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = NsdiFig1LtSimpleBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'simplebpaxos_nsdi_fig_1_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
