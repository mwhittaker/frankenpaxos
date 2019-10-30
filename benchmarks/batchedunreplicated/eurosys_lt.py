from .batchedunreplicated import *


def main(args) -> None:
    class SmokeBatchedUnreplicatedSuite(BatchedUnreplicatedSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    num_client_procs=num_client_procs,
                    num_warmup_clients_per_proc=num_clients_per_proc,
                    num_clients_per_proc=num_clients_per_proc,
                    num_batchers=num_batchers,
                    num_proxy_servers=num_proxy_servers,
                    client_jvm_heap_size='16g',
                    batcher_jvm_heap_size='32g',
                    server_jvm_heap_size='32g',
                    proxy_server_jvm_heap_size='32g',
                    warmup_duration=datetime.timedelta(seconds=5),
                    warmup_timeout=datetime.timedelta(seconds=10),
                    warmup_sleep=datetime.timedelta(seconds=5),
                    duration=datetime.timedelta(seconds=10),
                    timeout=datetime.timedelta(seconds=15),
                    client_lag=datetime.timedelta(seconds=5),
                    state_machine='Noop',
                    workload=workload.StringWorkload(size_mean=1, size_std=0),
                    profiled=args.profile,
                    monitored=args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                    client_options=ClientOptions(),
                    client_log_level=args.log_level,
                    batcher_options=BatcherOptions(
                        batch_size=batch_size
                    ),
                    batcher_log_level=args.log_level,
                    server_options=ServerOptions(
                        flush_every_n=server_flush_every_n,
                    ),
                    server_log_level=args.log_level,
                    proxy_server_options=ProxyServerOptions(
                        flush_every_n=proxy_server_flush_every_n
                    ),
                    proxy_server_log_level=args.log_level,
                )

                for (
                    num_client_procs,
                    num_clients_per_proc,
                    num_batchers,
                    num_proxy_servers,
                    batch_size,
                    server_flush_every_n,
                    proxy_server_flush_every_n
                ) in [
                    ( 1,   1, 1, 10, 1, 1, 1),
                    ( 1,  10, 1, 10, 5, 1, 1),
                    ( 5,  10, 1, 10, 10, 1, 1),
                    ( 5,  20, 2, 10, 20, 1, 1),
                    ( 6,  50, 3, 10, 50, 1, 1),
                    ( 6, 100, 4, 10, 100, 1, 1),
                    (12, 100, 5, 10, 100, 1, 1),
                ]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'num_batchers': input.num_batchers,
                'num_proxy_servers': input.num_proxy_servers,
                'batch_size': input.batcher_options.batch_size,
                'server.flush_every_n': input.server_options.flush_every_n,
                'proxy_serverserver.flush_every_n':
                input.proxy_server_options.flush_every_n,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.7}',
            })

    suite = SmokeBatchedUnreplicatedSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'batchedunreplicated_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
