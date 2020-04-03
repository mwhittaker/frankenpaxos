from .epaxos import *


def main(args) -> None:
    class SmokeEPaxosSuite(EPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = 1,
                    warmup_duration = datetime.timedelta(seconds=2),
                    warmup_timeout = datetime.timedelta(seconds=3),
                    warmup_sleep = datetime.timedelta(seconds=0),
                    duration = datetime.timedelta(seconds=2),
                    timeout = datetime.timedelta(seconds=3),
                    client_lag = datetime.timedelta(seconds=0),
                    state_machine = 'Register',
                    workload = workload.StringWorkload(size_mean=0, size_std=0),
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
                        execute_graph_batch_size = 1,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1),
                        num_blockers = 1,
                        top_k_dependencies = 1,
                    ),
                    replica_zigzag_options = ZigzagOptions(
                        vertices_grow_size = 1000,
                        garbage_collect_every_n_commands = 1000,
                    ),
                    replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(seconds=600),
                    ),
                    client_log_level = args.log_level,
                )
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'start_throughput_1s.p90': f'{output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeEPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'epaxos_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
