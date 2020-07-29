from .vanillamencius import *


def main(args) -> None:
    class SmokeVanillaMenciusSuite(VanillaMenciusSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = 1,
                    num_servers = 3,
                    client_jvm_heap_size = '100m',
                    server_jvm_heap_size = '100m',
                    measurement_group_size = 1,
                    warmup_duration = datetime.timedelta(seconds=2),
                    warmup_timeout = datetime.timedelta(seconds=3),
                    warmup_sleep = datetime.timedelta(seconds=0),
                    duration = datetime.timedelta(seconds=2),
                    timeout = datetime.timedelta(seconds=3),
                    client_lag = datetime.timedelta(seconds=3),
                    state_machine = 'KeyValueStore',
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys=1, size_mean=1, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    server_options = ServerOptions(),
                    server_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period = \
                            datetime.timedelta(seconds=120),
                    ),
                    client_log_level = args.log_level,
                )
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': \
                    f'{output.output.latency.median_ms:.6}',
                'start_throughput_1s.p90': \
                    f'{output.output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeVanillaMenciusSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'vanillamencius_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
