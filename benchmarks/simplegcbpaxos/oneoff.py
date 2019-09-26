from .simplegcbpaxos import *


def main(args) -> None:
    class OneOffSimpleGcBPaxosSuite(SimpleGcBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = f,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 50,
                    num_clients_per_proc = num_clients_per_proc,
                    num_leaders = 5,
                    warmup_duration = datetime.timedelta(seconds=15),
                    warmup_timeout = datetime.timedelta(seconds=20),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=15),
                    timeout = datetime.timedelta(seconds=20),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = state_machine,
                    workload = wl,
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    leader_options = LeaderOptions(
                        thrifty_system = 'Random',
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
                        garbage_collect_every_n_commands = 20000,
                        unsafe_return_no_dependencies = unsafe_return_no_dependencies,
                    ),
                    dep_service_node_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(
                        states_grow_size = 20000,
                    ),
                    acceptor_log_level = args.log_level,
                    replica_options = ReplicaOptions(
                        unsafe_skip_graph_execution = unsafe_skip_graph_execution,
                        commands_grow_size = 20000,
                        recover_vertex_timer_min_period = \
                            datetime.timedelta(seconds=600),
                        recover_vertex_timer_max_period = \
                            datetime.timedelta(seconds=600),
                        execute_graph_batch_size = 100,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1),
                        send_watermark_every_n_commands = 1000,
                        send_snapshot_every_n_commands = 5000,
                        unsafe_dont_recover = unsafe_dont_recover,
                        num_blockers = 1,
                    ),
                    replica_zigzag_options = ZigzagOptions(
                        vertices_grow_size = 20000,
                        garbage_collect_every_n_commands = 20000,
                    ),
                    replica_log_level = args.log_level,
                    garbage_collector_options = GarbageCollectorOptions(),
                    garbage_collector_log_level = args.log_level,
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(seconds=600),
                    ),
                    client_log_level = args.log_level,
                )
                for f in [1]
                for (state_machine, wl) in [
                    (
                        'KeyValueStore',
                         workload.UniformSingleKeyWorkload(
                             num_keys = 10000,
                             size_mean = 1,
                             size_std = 0,
                         ),
                    ),
                    # (
                    #     'Noop',
                    #      workload.StringWorkload(
                    #          size_mean = 1,
                    #          size_std = 0,
                    #      ),
                    # ),
                ]
                for (unsafe_dont_recover, unsafe_return_no_dependencies, unsafe_skip_graph_execution) in [
                    # (False, True, False),
                    (False, False, False),
                ]
                for (num_client_procs, num_clients_per_proc) in
                    [(6, 100)]
                    # [(1, 1)]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = OneOffSimpleGcBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'simplegcbpaxos_oneoff') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
