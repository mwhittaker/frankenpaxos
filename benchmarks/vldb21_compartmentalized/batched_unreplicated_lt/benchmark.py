from ...batchedunreplicated.batchedunreplicated import *


def main(args) -> None:
    class Suite(BatchedUnreplicatedSuite):
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
                    client_jvm_heap_size='8g',
                    batcher_jvm_heap_size='12g',
                    server_jvm_heap_size='12g',
                    proxy_server_jvm_heap_size='12g',
                    measurement_group_size=100,
                    warmup_duration=datetime.timedelta(seconds=10),
                    warmup_timeout=datetime.timedelta(seconds=15),
                    warmup_sleep=datetime.timedelta(seconds=5),
                    duration=datetime.timedelta(seconds=15),
                    timeout=datetime.timedelta(seconds=20),
                    client_lag=datetime.timedelta(seconds=5),
                    state_machine='KeyValueStore',
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys=1,
                        size_mean=16,
                        size_std=0),
                    profiled=args.profile,
                    monitored=args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                    client_options=ClientOptions(),
                    client_log_level=args.log_level,
                    batcher_options=BatcherOptions(batch_size=batch_size),
                    batcher_log_level=args.log_level,
                    server_options=ServerOptions(
                        flush_every_n=server_flush_every_n
                    ),
                    server_log_level=args.log_level,
                    proxy_server_options=ProxyServerOptions(
                        flush_every_n=proxy_server_flush_every_n
                    ),
                    proxy_server_log_level=args.log_level,
                )

                # Hyperparamter tuning.
                #
                # - 4 proxy servers is better than 3, even though 3 is good
                #   enough for MultiPaxos.
                # - 2 batchers seems like enough.
                # for batch_size in [20, 40, 60, 80, 100]
                # for (
                #     num_batchers,               # 0
                #     num_proxy_servers,          # 1
                #     batch_size,                 # 2
                #     server_flush_every_n,       # 3
                #     proxy_server_flush_every_n, # 4
                #     num_client_procs,           # 5
                #     num_clients_per_proc,       # 6
                # ) in [
                #     # 0  1           2  3           4   5    6
                #     ( 2, 4, batch_size, 1, batch_size, 40, 100),
                # ]

                for (
                    num_batchers,               # 0
                    num_proxy_servers,          # 1
                    batch_size,                 # 2
                    server_flush_every_n,       # 3
                    proxy_server_flush_every_n, # 4
                    num_client_procs,           # 5
                    num_clients_per_proc,       # 6
                ) in [
                    # 0  1    2  3    4   5    6
                    ( 2, 4,   1, 1,   1,  1,   1),
                    ( 2, 4,  10, 1,  10,  1,  50),
                    ( 2, 4,  20, 1,  20,  1, 100),
                    ( 2, 4,  40, 1,  40,  2, 100),
                    ( 2, 4,  40, 1,  40,  3, 100),
                    ( 2, 4, 100, 1, 100,  4, 100),
                    ( 2, 4, 100, 1, 100,  5, 100),
                    ( 2, 4, 100, 1, 100,  6, 100),
                    ( 2, 4, 100, 1, 100,  7, 100),
                    ( 2, 4, 100, 1, 100,  8, 100),
                    ( 2, 4, 100, 1, 100,  9, 100),
                    ( 2, 4, 100, 1, 100, 10, 100),
                    ( 2, 4, 100, 1, 100, 12, 100),
                    ( 2, 4, 100, 1, 100, 14, 100),
                    ( 2, 4, 100, 1, 100, 16, 100),
                    ( 2, 4, 100, 1, 100, 18, 100),
                    ( 2, 4, 100, 1, 100, 20, 100),
                    ( 2, 4, 100, 1, 100, 25, 100),
                    ( 2, 4, 100, 1, 100, 30, 100),
                    ( 2, 4, 100, 1, 100, 35, 100),
                    ( 2, 4, 100, 1, 100, 40, 100),
                ]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_client_procs':
                    input.num_client_procs,
                'num_clients_per_proc':
                    input.num_clients_per_proc,
                'num_batchers':
                    input.num_batchers,
                'num_proxy_servers':
                    input.num_proxy_servers,
                'batch_size':
                    input.batcher_options.batch_size,
                'server_flush_every_n':
                    input.server_options.flush_every_n,
                'proxy_server_flush_every_n':
                    input.proxy_server_options.flush_every_n,
                'latency.median_ms': \
                    f'{output.latency.median_ms:.6}',
                'start_throughput_1s.p90': \
                    f'{output.start_throughput_1s.p90:.8}',
            })

    suite = Suite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'batchedunreplicated_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
