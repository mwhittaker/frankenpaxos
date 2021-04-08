from .unreplicated import *


def main(args) -> None:
    class SmokeUnreplicatedSuite(UnreplicatedSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    num_client_procs=num_client_procs,
                    num_warmup_clients_per_proc=1,
                    num_clients_per_proc=1,
                    jvm_heap_size='100m',
                    measurement_group_size=1,
                    warmup_duration=datetime.timedelta(seconds=2),
                    warmup_timeout=datetime.timedelta(seconds=3),
                    warmup_sleep=datetime.timedelta(seconds=0),
                    duration=datetime.timedelta(seconds=2),
                    timeout=datetime.timedelta(seconds=3),
                    client_lag=datetime.timedelta(seconds=0),
                    state_machine='Noop',
                    workload=workload.StringWorkload(size_mean=1, size_std=0),
                    profiled=args.profile,
                    monitored=args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                    client_options=ClientOptions(),
                    client_log_level=args.log_level,
                    server_options=ServerOptions(),
                    server_log_level=args.log_level,
                )

                for num_client_procs in [1, 2, 3]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'start_throughput_1s.p90': f'{output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeUnreplicatedSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'unreplicated_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
