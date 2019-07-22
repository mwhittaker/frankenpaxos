from .epaxos import *


def main(args) -> None:
    class LtEPaxosSuite(EPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = f,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = num_clients_per_proc,
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
                    replica_options = ReplicaOptions(
                        thrifty_system =thrifty_system,
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
                        execute_graph_batch_size = 100,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1)
                    ),
                    replica_log_level = args.log_level,
                    replica_dependency_graph = dependency_graph,
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(seconds=60),
                    ),
                    client_log_level = args.log_level,
                )
                for (f, dependency_graph, thrifty_system) in [
                    # Sweep the dependency graph.
                    (1, 'Tarjan', 'Random'),
                    (1, 'IncrementalTarjan', 'Random'),
                    (1, 'Jgrapht', 'Random'),
                    # Sweep the thriftiness.
                    (1, 'Tarjan', 'NotThrifty'),
                    # Sweep f.
                    (2, 'Tarjan', 'Random'),
                ]
                for (num_client_procs, num_clients_per_proc) in
                    [
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
                'dep_graph': input.replica_dependency_graph,
                'thrifty_system': input.replica_options.thrifty_system,
                'num_clients':
                    (input.num_client_procs, input.num_clients_per_proc),
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = LtEPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'epaxos_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
