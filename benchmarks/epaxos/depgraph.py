from .epaxos import *


def main(args) -> None:
    class DepGraphEPaxosSuite(EPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 10,
                    num_clients_per_proc = num_clients_per_proc,
                    warmup_duration = datetime.timedelta(seconds=5),
                    warmup_timeout = datetime.timedelta(seconds=10),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=20),
                    timeout = datetime.timedelta(seconds=60),
                    client_lag = datetime.timedelta(seconds=5),
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
                    replica_options = ReplicaOptions(
                        resend_pre_accepts_timer_period = \
                            datetime.timedelta(seconds=60),
                        default_to_slow_path_timer_period = \
                            datetime.timedelta(seconds=60),
                        resend_accepts_timer_period = \
                            datetime.timedelta(seconds=60),
                        resend_prepares_timer_period = \
                            datetime.timedelta(seconds=60),
                        recover_instance_timer_min_period = \
                            datetime.timedelta(seconds=60),
                        recover_instance_timer_max_period = \
                            datetime.timedelta(seconds=120),
                        unsafe_skip_graph_execution =
                            unsafe_skip_graph_execution,
                        execute_graph_batch_size = 100,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1)
                    ),
                    replica_log_level = args.log_level,
                    replica_dependency_graph =
                        replica_dependency_graph,
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(seconds=60),
                    ),
                    client_log_level = args.log_level,
                )
                for unsafe_skip_graph_execution in [True, False]
                for replica_dependency_graph in (
                    ["Jgrapht", "Tarjan", "IncrementalTarjan"]
                    if not unsafe_skip_graph_execution else ["Tarjan"])
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

    suite = DepGraphEPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'epaxos_dep_graph') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
