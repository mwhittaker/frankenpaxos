from .epaxos import *


def main(args) -> None:
    class NsdiFig1LtEPaxosSuite(EPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = f,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 50,
                    num_clients_per_proc = num_clients_per_proc,
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
                    replica_options = ReplicaOptions(
                        thrifty_system = 'Random',
                        resend_pre_accepts_timer_period = \
                            datetime.timedelta(seconds=600),
                        default_to_slow_path_timer_period = \
                            datetime.timedelta(seconds=600),
                        resend_accepts_timer_period = \
                            datetime.timedelta(seconds=600),
                        resend_prepares_timer_period = \
                            datetime.timedelta(seconds=600),
                        recover_instance_timer_min_period = \
                            datetime.timedelta(seconds=600),
                        recover_instance_timer_max_period = \
                            datetime.timedelta(seconds=1200),
                        execute_graph_batch_size = execute_graph_batch_size,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1),
                        num_blockers = 1,
                        top_k_dependencies = 1,
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
                for conflict_rate in [0.0, 0.02, 0.1, 0.20]
                for load in [
                    workload.BernoulliSingleKeyWorkload(
                        conflict_rate = conflict_rate,
                        size_mean = 8,
                        size_std = 0,
                    )
                ]
                for (num_client_procs, num_clients_per_proc) in
                    [
                        (1, 1),
                        (5, 10),
                        (5, 20),
                        (6, 50),
                        (6, 100),
                        (12, 100),
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
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'execute_graph_batch_size':
                    input.replica_options.execute_graph_batch_size,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = NsdiFig1LtEPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'epaxos_nsdi_fig1_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
