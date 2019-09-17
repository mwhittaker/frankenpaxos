from .epaxos import *


def main(args) -> None:
    class NsdiFig3WanEPaxosSuite(EPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 50,
                    num_clients_per_proc = num_clients_per_proc,
                    warmup_duration = datetime.timedelta(seconds=15),
                    warmup_timeout = datetime.timedelta(seconds=20),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=25),
                    timeout = datetime.timedelta(seconds=30),
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
                for (num_client_procs, num_clients_per_proc) in [(1, 1)]
                for execute_graph_batch_size in [1]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = NsdiFig3WanEPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'epaxos_nsdi_fig3_wan') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
