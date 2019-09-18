from .simplebpaxos import *


def main(args) -> None:
    class NsdiFig2AblationSimpleBPaxosSuite(SimpleBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
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
                    workload = workload.BernoulliSingleKeyWorkload(
                        conflict_rate = 0.0,
                        size_mean = 8,
                        size_std = 0,
                    ),
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
                for num_leaders in [3, 4, 5, 6, 7, 8, 9, 10]
                for (num_client_procs, num_clients_per_proc) in
                    [(1, 1), (6, 100)]
                for execute_graph_batch_size in (
                    [1] if num_client_procs * num_clients_per_proc == 1 else
                    [100] if num_client_procs * num_clients_per_proc > 100 else
                    [int(num_client_procs * num_clients_per_proc / 2)]
                )
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_leaders': input.num_leaders,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'execute_graph_batch_size':
                    input.replica_options.execute_graph_batch_size,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = NsdiFig2AblationSimpleBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'simplebpaxos_nsdi_fig_2_ablation') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
