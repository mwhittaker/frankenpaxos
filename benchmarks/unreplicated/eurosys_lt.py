from .unreplicated import *


def main(args) -> None:
    class EuroSysLtUnreplicatedSuite(UnreplicatedSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    num_client_procs=num_client_procs,
                    num_warmup_clients_per_proc=num_clients_per_proc,
                    num_clients_per_proc=num_clients_per_proc,
                    jvm_heap_size='32g',
                    warmup_duration=datetime.timedelta(seconds=5),
                    warmup_timeout=datetime.timedelta(seconds=10),
                    warmup_sleep=datetime.timedelta(seconds=5),
                    duration=datetime.timedelta(seconds=8),
                    timeout=datetime.timedelta(seconds=13),
                    client_lag=datetime.timedelta(seconds=5),
                    state_machine='Noop',
                    workload=workload.StringWorkload(size_mean=1, size_std=0),
                    profiled=args.profile,
                    monitored=args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                    client_options=ClientOptions(),
                    client_log_level=args.log_level,
                    server_options=ServerOptions(flush_every_n=flush_every_n,),
                    server_log_level=args.log_level,
                )
                for (num_client_procs, num_clients_per_proc, flush_every_n) in [
                    (1, 1, 1),
                    (1, 10, 5),
                    (5, 10, 10),
                    (5, 20, 20),
                    (6, 50, 50),
                    (6, 100, 100),
                    (10, 100, 100),
                    (20, 100, 100),
                    (20, 200, 100),
                ]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'flush_every_n': input.server_options.flush_every_n,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.7}',
            })

    suite = EuroSysLtUnreplicatedSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'unreplicated_eurosys_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
