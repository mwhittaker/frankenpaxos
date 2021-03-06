from ...scalog.scalog import *


def main(args) -> None:
    class Suite(ScalogSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return ([
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = num_clients_per_proc,
                    num_clients_per_proc = num_clients_per_proc,
                    num_shards = num_shards,
                    num_servers_per_shard = 2,
                    num_leaders = 2,
                    num_acceptors = 3,
                    num_replicas = num_replicas,
                    num_proxy_replicas = num_proxy_replicas,
                    client_jvm_heap_size = '8g',
                    server_jvm_heap_size = '12g',
                    aggregator_jvm_heap_size = '12g',
                    leader_jvm_heap_size = '12g',
                    acceptor_jvm_heap_size = '12g',
                    replica_jvm_heap_size = '12g',
                    proxy_replica_jvm_heap_size = '12g',
                    measurement_group_size = 100,
                    warmup_duration = datetime.timedelta(seconds=10),
                    warmup_timeout = datetime.timedelta(seconds=15),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=15),
                    timeout = datetime.timedelta(seconds=20),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    workload_label = workload_label,
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys=1, size_mean=16, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    server_options = ServerOptions(
                        push_size = push_size,
                        push_period = push_period,
                        recover_period = datetime.timedelta(seconds=1),
                    ),
                    server_log_level = args.log_level,
                    aggregator_options = AggregatorOptions(
                        num_shard_cuts_per_proposal = \
                            num_shard_cuts_per_proposal,
                        recover_period = datetime.timedelta(seconds=1),
                        leader_info_period = datetime.timedelta(seconds=60),
                    ),
                    aggregator_log_level = args.log_level,
                    leader_options = LeaderOptions(
                        resend_phase1as_period = datetime.timedelta(seconds=1),
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
                            datetime.timedelta(seconds=1),
                        recover_log_entry_max_period = \
                            datetime.timedelta(seconds=5),
                        unsafe_yolo_execution = yolo,
                        unsafe_round_robin_by_chunk = yolo,
                    ),
                    replica_log_level = args.log_level,
                    proxy_replica_options = ProxyReplicaOptions(
                        batch_flush = batch_flush,
                    ),
                    proxy_replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period = \
                            datetime.timedelta(seconds=1),
                    ),
                    client_log_level = args.log_level,
                )

                # Hyperparamter tuning.
                #
                # - Without batch flushing and with 2 replicas, everything
                #   seems to bottleneck at just below 200,000 requests per
                #   second. Looking at grafana, every non-server component has
                #   very low inbound load. I think the replicas replying to the
                #   clients are the bottleneck here.
                # - Adding more replicas and adding batch flushing didn't
                #   increase throughput.
                # - Adding more shards does increase throughput, but we need a
                #   lot more clients to fully load it.
                # - Push period has a big impact on throughput at lower number
                #   of clients.
                # - No proxy replicas, sweep to 20.
                # - Proxy leaders don't really help.
                # - Yolo helps a bit but not as much as I'd like.
                # - 125 best batch size.
                # for workload_label in ['no_yolo_v1']
                # for num_shard_cuts_per_proposal in [1]
                # for yolo in [False]
                # for push_size in [100]
                # for ns in [3]
                # for nr in [8]
                # for npr in [0]
                # for (
                #     num_shards,           # 0
                #     push_period_ms,       # 1
                #     num_replicas,         # 2
                #     batch_flush,          # 3
                #     num_proxy_replicas,   # 4
                #     num_client_procs,     # 5
                #     num_clients_per_proc, # 6
                # ) in [
                #     # 0    1   2     3  4   5    6
                #     (ns, 100, nr, True, npr,  5, 100),
                #     (ns, 100, nr, True, npr, 10, 100),
                #     (ns, 100, nr, True, npr, 15, 100),
                #     (ns, 100, nr, True, npr, 20, 100),
                #     (ns, 100, nr, True, npr, 30, 100),
                # ]
                #
                # for push_period in [
                #     datetime.timedelta(milliseconds=push_period_ms)
                # ]

                for workload_label in ['final_v2']
                for num_shard_cuts_per_proposal in [1]
                for push_size in [100]
                for (
                    num_shards,           # 0
                    push_period_ms,       # 1
                    num_replicas,         # 2
                    batch_flush,          # 3
                    num_proxy_replicas,   # 4
                    num_client_procs,     # 5
                    num_clients_per_proc, # 6
                    yolo,                 # 7
                ) in [
                    # 0    1  2     3  4   5    6     7
                    ( 3, 100, 5, True, 0,  1,   1, False),
                    ( 3, 100, 5, True, 0,  1,  50, False),
                    ( 3, 100, 5, True, 0,  1, 100, False),
                    ( 3, 100, 5, True, 0,  2, 100, False),
                    ( 3, 100, 5, True, 0,  3, 100, False),
                    ( 3, 100, 5, True, 0,  4, 100, False),
                    ( 3, 100, 5, True, 0,  5, 100, False),
                    ( 3, 100, 5, True, 0,  6, 100, False),
                    ( 3, 100, 5, True, 0,  7, 100, False),
                    ( 3, 100, 5, True, 0,  8, 100, False),
                    ( 3, 100, 5, True, 0,  9, 100, False),
                    ( 3, 100, 5, True, 0, 10, 100, False),
                    ( 3, 100, 5, True, 0, 12, 100, False),
                    ( 3, 100, 5, True, 0, 14, 100, False),
                    ( 3, 100, 5, True, 0, 16, 100, False),
                    ( 3, 100, 5, True, 0, 18, 100, False),
                    ( 3, 100, 5, True, 0, 20, 100, False),
                    ( 3, 100, 5, True, 0, 25, 100, False),
                    ( 3, 100, 5, True, 0, 30, 100, False),
                    ( 3, 100, 5, True, 0, 35, 100, False),
                    ( 3, 100, 5, True, 0, 40, 100, False),

                    # # 0    1  2     3  4   5    6     7
                    # ( 4, 100, 2, True, 4,  1,   1, True),
                    # ( 4, 100, 2, True, 4,  1,  50, True),
                    # ( 4, 100, 2, True, 4,  1, 100, True),
                    # ( 4, 100, 2, True, 4,  2, 100, True),
                    # ( 4, 100, 2, True, 4,  3, 100, True),
                    # ( 4, 100, 2, True, 4,  4, 100, True),
                    # ( 4, 100, 2, True, 4,  5, 100, True),
                    # ( 4, 100, 2, True, 4,  6, 100, True),
                    # ( 4, 100, 2, True, 4,  7, 100, True),
                    # ( 4, 100, 2, True, 4,  8, 100, True),
                    # ( 4, 100, 2, True, 4,  9, 100, True),
                    # ( 4, 100, 2, True, 4, 10, 100, True),
                    # ( 4, 100, 2, True, 4, 12, 100, True),
                    # ( 4, 100, 2, True, 4, 14, 100, True),
                    # ( 4, 100, 2, True, 4, 16, 100, True),
                    # ( 4, 100, 2, True, 4, 18, 100, True),
                    # ( 4, 100, 2, True, 4, 20, 100, True),
                    # ( 4, 100, 2, True, 4, 25, 100, True),
                    # ( 4, 100, 2, True, 4, 30, 100, True),
                    # ( 4, 100, 2, True, 4, 35, 100, True),
                    # ( 4, 100, 2, True, 4, 40, 100, True),
                ]

                for push_period in [
                    datetime.timedelta(milliseconds=push_period_ms)
                ]
            ] * 5)[81:]

        def summary(self, input: Input, output: Output) -> str:
            push_period_s = input.server_options.push_period.total_seconds()
            return str({
                'num_client_procs':
                    input.num_client_procs,
                'num_clients_per_proc':
                    input.num_clients_per_proc,
                'num_shards':
                    input.num_shards,
                'push_size':
                    f'{input.server_options.push_size}',
                'push_period':
                    f'{push_period_s * 1000}ms',
                'num_replicas':
                    input.num_replicas,
                'num_proxy_replicas':
                    input.num_proxy_replicas,
                'batch_flush':
                    input.replica_options.batch_flush,
                'num_shard_cuts_per_proposal':
                    input.aggregator_options.num_shard_cuts_per_proposal,
                'yolo':
                    input.replica_options.unsafe_yolo_execution,
                'latency.median_ms': \
                    f'{output.output.latency.median_ms:.6}',
                'start_throughput_1s.p90': \
                    f'{output.output.start_throughput_1s.p90:.8}',
            })

    suite = Suite()
    with benchmark.SuiteDirectory(args.suite_directory, 'scalog_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
