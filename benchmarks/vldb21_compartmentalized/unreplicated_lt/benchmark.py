from ...unreplicated.unreplicated import *


def main(args) -> None:
    class Suite(UnreplicatedSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    num_client_procs=num_client_procs,
                    num_warmup_clients_per_proc=num_clients_per_proc,
                    num_clients_per_proc=num_clients_per_proc,
                    jvm_heap_size='12g',
                    measurement_group_size = 10,
                    warmup_duration=datetime.timedelta(seconds=10),
                    warmup_timeout=datetime.timedelta(seconds=15),
                    warmup_sleep=datetime.timedelta(seconds=5),
                    duration=datetime.timedelta(seconds=15),
                    timeout=datetime.timedelta(seconds=20),
                    client_lag=datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys=1,
                        size_mean=value_size,
                        size_std=0),
                    profiled=args.profile,
                    monitored=args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                    client_options=ClientOptions(),
                    client_log_level=args.log_level,
                    server_options=ServerOptions(flush_every_n=flush_every_n),
                    server_log_level=args.log_level,
                )

                # Hyperparameter tuning.
                # for value_size in [100, 1000]
                # for flush_every_n in [1, 10, 25, 50, 100]
                # for (num_client_procs, num_clients_per_proc) in [
                #     (1, 100),
                #     (2, 100),
                #     (4, 100),
                #     (8, 100),
                #     (12, 100),
                #     (16, 100),
                #     (20, 100),
                # ]

                # # Benchmark.
                for (
                    num_client_procs,
                    num_clients_per_proc,
                    value_size,
                    flush_every_n,
                ) in [
                    # ( 1,   1, 16, 1),
                    # ( 1,  10, 16, 1),
                    # ( 1,  50, 16, 25),
                    # ( 1, 100, 16, 25),
                    # ( 2, 100, 16, 25),
                    # ( 3, 100, 16, 25),
                    # ( 4, 100, 16, 25),
                    # ( 5, 100, 16, 25),
                    # ( 6, 100, 16, 25),
                    # ( 7, 100, 16, 25),
                    # ( 8, 100, 16, 25),
                    # (11, 100, 16, 25),
                    # (14, 100, 16, 25),

                    ( 1,   1, 100, 1),
                    ( 1,  10, 100, 1),
                    ( 1,  50, 100, 25),
                    ( 1, 100, 100, 25),
                    ( 2, 100, 100, 25),
                    ( 3, 100, 100, 25),
                    ( 4, 100, 100, 25),
                    ( 5, 100, 100, 25),
                    ( 6, 100, 100, 25),
                    ( 7, 100, 100, 25),
                    ( 8, 100, 100, 25),
                    ( 9, 100, 100, 25),
                    (10, 100, 100, 25),
                    (11, 100, 100, 25),
                    (12, 100, 100, 25),
                    (13, 100, 100, 25),
                    (14, 100, 100, 25),

                    ( 1,   1, 1000, 1),
                    ( 1,  10, 1000, 1),
                    ( 1,  50, 1000, 10),
                    ( 1, 100, 1000, 10),
                    ( 2, 100, 1000, 10),
                    ( 3, 100, 1000, 10),
                    ( 4, 100, 1000, 10),
                    ( 5, 100, 1000, 10),
                    ( 6, 100, 1000, 10),
                    ( 7, 100, 1000, 10),
                    ( 8, 100, 1000, 10),
                    ( 9, 100, 1000, 10),
                    (10, 100, 1000, 10),
                    (11, 100, 1000, 10),
                    (12, 100, 1000, 10),
                    (13, 100, 1000, 10),
                    (14, 100, 1000, 10),
                ]
            ] * 5

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'flush_every_n': input.server_options.flush_every_n,
                'value_size': input.workload.size_mean,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'start_throughput_1s.p90': f'{output.start_throughput_1s.p90:.7}',
            })

    suite = Suite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'unreplicated_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
