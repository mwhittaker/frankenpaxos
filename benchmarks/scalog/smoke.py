from .scalog import *


def main(args) -> None:
    class SmokeScalogSuite(ScalogSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 5,
                    num_clients_per_proc = 5,
                    num_shards = num_shards,
                    num_servers_per_shard = 2,
                    num_leaders = 2,
                    num_acceptors = 3,
                    num_replicas = 2,
                    num_proxy_replicas = num_proxy_replicas,
                    client_jvm_heap_size = '100m',
                    server_jvm_heap_size = '100m',
                    aggregator_jvm_heap_size = '100m',
                    leader_jvm_heap_size = '100m',
                    acceptor_jvm_heap_size = '100m',
                    replica_jvm_heap_size = '100m',
                    proxy_replica_jvm_heap_size = '100m',
                    measurement_group_size = 1,
                    warmup_duration = datetime.timedelta(seconds=2),
                    warmup_timeout = datetime.timedelta(seconds=3),
                    warmup_sleep = datetime.timedelta(seconds=0),
                    duration = datetime.timedelta(seconds=2),
                    timeout = datetime.timedelta(seconds=3),
                    client_lag = datetime.timedelta(seconds=3),
                    state_machine = 'KeyValueStore',
                    workload_label = 'smoke',
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys=1, size_mean=1, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    server_options = ServerOptions(
                        push_size = push_size,
                        push_period = datetime.timedelta(milliseconds=50),
                        recover_period = datetime.timedelta(seconds=60),
                    ),
                    server_log_level = args.log_level,
                    aggregator_options = AggregatorOptions(
                        num_shard_cuts_per_proposal = num_shards * 2,
                        recover_period = datetime.timedelta(seconds=60),
                        leader_info_period = datetime.timedelta(seconds=60),
                    ),
                    aggregator_log_level = args.log_level,
                    leader_options = LeaderOptions(
                        resend_phase1as_period = datetime.timedelta(seconds=60),
                        flush_phase2as_every_n = 1,
                        election_options = ElectionOptions(
                            ping_period = datetime.timedelta(seconds=60),
                            no_ping_timeout_min = \
                                datetime.timedelta(seconds=120),
                            no_ping_timeout_max = \
                                datetime.timedelta(seconds=240),
                        ),
                    ),
                    leader_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    replica_options = ReplicaOptions(
                        log_grow_size = 5000,
                        batch_flush = batch_flush,
                        recover_log_entry_min_period = \
                            datetime.timedelta(seconds=120),
                        recover_log_entry_max_period = \
                            datetime.timedelta(seconds=240),
                        unsafe_yolo_execution = yolo,
                    ),
                    replica_log_level = args.log_level,
                    proxy_replica_options = ProxyReplicaOptions(
                        batch_flush = batch_flush,
                    ),
                    proxy_replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period = \
                            datetime.timedelta(seconds=120),
                    ),
                    client_log_level = args.log_level,
                )
                for (push_size, num_shards, num_proxy_replicas, batch_flush,
                     yolo) in [
                    (1, 1, 0, False, False),
                    (0, 2, 2, True, True),
                ]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'num_shards': input.num_shards,
                'num_servers_per_shard': input.num_servers_per_shard,
                'num_proxy_replicas': input.num_proxy_replicas,
                'batch_flush': input.replica_options.batch_flush,
                'latency.median_ms': \
                    f'{output.output.latency.median_ms:.6}',
                'start_throughput_1s.p90': \
                    f'{output.output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeScalogSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'scalog_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
