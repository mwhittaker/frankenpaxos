from ...multipaxos.multipaxos import *


def main(args) -> None:
    class Suite(MultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = num_clients_per_proc,
                    num_clients_per_proc = num_clients_per_proc,
                    num_batchers = 0,
                    num_read_batchers = 0,
                    num_leaders = 2,
                    num_proxy_leaders = num_proxy_leaders,
                    num_acceptor_groups = num_acceptor_groups,
                    num_acceptors_per_group = num_acceptors_per_group,
                    num_replicas = num_replicas,
                    num_proxy_replicas = 0,
                    flexible = flexible,
                    distribution_scheme = DistributionScheme.HASH,
                    client_jvm_heap_size = '8g',
                    batcher_jvm_heap_size = '12g',
                    read_batcher_jvm_heap_size = '12g',
                    leader_jvm_heap_size = '12g',
                    proxy_leader_jvm_heap_size = '12g',
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
                    predetermined_read_fraction = -1,
                    workload_label = workload_label,
                    workload =
                      read_write_workload.UniformMultiKeyReadWriteWorkload(
                        num_keys=batch_size,
                        num_operations=batch_size,
                        read_fraction=read_fraction,
                        write_size_mean=16,
                        write_size_std=0),
                    read_workload =
                      read_write_workload.UniformMultiKeyReadWriteWorkload(
                        num_keys=batch_size,
                        num_operations=batch_size,
                        read_fraction=1.0,
                        write_size_mean=16,
                        write_size_std=0),
                    write_workload =
                      read_write_workload.UniformMultiKeyReadWriteWorkload(
                        num_keys=batch_size,
                        num_operations=batch_size,
                        read_fraction=0.0,
                        write_size_mean=16,
                        write_size_std=0),
                    read_consistency = 'linearizable',
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    batcher_options = BatcherOptions(),
                    batcher_log_level = args.log_level,
                    read_batcher_options = ReadBatcherOptions(),
                    read_batcher_log_level = args.log_level,
                    leader_options = LeaderOptions(
                        resend_phase1as_period = datetime.timedelta(seconds=1),
                        flush_phase2as_every_n = leader_flush_every_n,
                        election_options = ElectionOptions(
                            ping_period = datetime.timedelta(seconds=60),
                            no_ping_timeout_min = \
                                datetime.timedelta(seconds=120),
                            no_ping_timeout_max = \
                                datetime.timedelta(seconds=240),
                        ),
                    ),
                    leader_log_level = args.log_level,
                    proxy_leader_options = ProxyLeaderOptions(
                        flush_phase2as_every_n = proxy_leader_flush_every_n,
                    ),
                    proxy_leader_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    replica_options = ReplicaOptions(
                        log_grow_size = 5000,
                        unsafe_dont_use_client_table = False,
                        send_chosen_watermark_every_n_entries = 100,
                        recover_log_entry_min_period = \
                            datetime.timedelta(seconds=2),
                        recover_log_entry_max_period = \
                            datetime.timedelta(seconds=5),
                        unsafe_dont_recover = False,
                    ),
                    replica_log_level = args.log_level,
                    proxy_replica_options = ProxyReplicaOptions(),
                    proxy_replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period =
                            datetime.timedelta(seconds=1),
                        resend_max_slot_requests_period =
                            datetime.timedelta(seconds=1),
                        resend_read_request_period =
                            datetime.timedelta(seconds=1),
                        resend_sequential_read_request_period =
                            datetime.timedelta(seconds=1),
                        resend_eventual_read_request_period =
                            datetime.timedelta(seconds=1),
                        unsafe_read_at_first_slot = False,
                        unsafe_read_at_i = False,
                        flush_writes_every_n = 1,
                        flush_reads_every_n = 1,
                    ),
                    client_log_level = args.log_level,
                )

                # Hyperparameter tuning.
                for workload_label in ['read_only_v3']
                for num_replicas in [2, 3, 4, 5, 6]
                for (num_client_procs, num_clients_per_proc) in [
                    (15, 100),
                    (20, 100),
                    (25, 100),
                    (30, 100),
                    (35, 100),
                    (40, 100),
                ]
                for (
                    num_proxy_leaders,          # 0
                    flexible,                   # 1
                    num_acceptor_groups,        # 2
                    num_acceptors_per_group,    # 3
                    num_replicas,               # 4
                    leader_flush_every_n,       # 5
                    proxy_leader_flush_every_n, # 6
                    read_fraction,              # 7
                    batch_size,                 # 8
                    num_client_procs,           # 9
                    num_clients_per_proc,       # 10
                ) in [
                    # 0     1             2  3             4  5  6    7   8                 9                    10
                    ( 2, True, num_replicas, 2, num_replicas, 1, 1, 1.0, 50, num_client_procs, num_clients_per_proc),
                ]

                # # Benchmark.
                # for workload_label in ['read_scale_v1']
                # for (
                #     num_proxy_leaders,          # 0
                #     flexible,                   # 1
                #     num_acceptor_groups,        # 2
                #     num_acceptors_per_group,    # 3
                #     num_replicas,               # 4
                #     leader_flush_every_n,       # 5
                #     proxy_leader_flush_every_n, # 6
                #     read_fraction,              # 7
                #     num_client_procs,           # 8
                #     num_clients_per_proc,       # 9
                # ) in [
                #     # 100% reads
                #     # 0     1  2  3  4  5  6    7   8   9
                #     ( 2, True, 2, 2, 2, 1, 1, 1.0, 15, 100),
                #     ( 2, True, 3, 2, 3, 1, 1, 1.0, 20, 100),
                #     ( 2, True, 4, 2, 4, 1, 1, 1.0, 25, 100),
                #     ( 2, True, 5, 2, 5, 1, 1, 1.0, 30, 100),
                #     ( 2, True, 6, 2, 6, 1, 1, 1.0, 35, 100),
                #
                #     # 90% reads
                #     # 0     1  2  3  4   5  6    7   8   9
                #     ( 5, True, 2, 2, 2, 10, 1, 0.9, 25, 100),
                #     ( 5, True, 3, 2, 3, 10, 1, 0.9, 25, 100),
                #     ( 5, True, 4, 2, 4, 10, 1, 0.9, 25, 100),
                #     ( 5, True, 5, 2, 5, 10, 1, 0.9, 25, 100),
                #     ( 5, True, 6, 2, 6, 10, 1, 0.9, 25, 100),
                #
                #     # 60% reads
                #     # 0     1  2  3  4   5  6    7   8   9
                #     ( 7, True, 2, 2, 2, 10, 1, 0.6, 15, 100),
                #     ( 7, True, 3, 2, 3, 10, 1, 0.6, 15, 100),
                #     ( 7, True, 4, 2, 4, 10, 1, 0.6, 15, 100),
                #     (11, True, 5, 2, 5, 10, 1, 0.6, 15, 100),
                #     (11, True, 6, 2, 6, 10, 1, 0.6, 15, 100),
                #
                #     # 0% reads (100% writes)
                #     # 0     1  2  3  4  5  6    7  8    9
                #     ( 6, True, 2, 2, 2, 1, 1, 0.0, 5, 100),
                #     ( 6, True, 3, 2, 3, 1, 1, 0.0, 5, 100),
                #     ( 6, True, 4, 2, 4, 1, 1, 0.0, 5, 100),
                #     ( 6, True, 5, 2, 5, 1, 1, 0.0, 5, 100),
                #     ( 6, True, 6, 2, 6, 1, 1, 0.0, 5, 100),
                # ]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_client_procs':
                    input.num_client_procs,
                'num_clients_per_proc':
                    input.num_clients_per_proc,
                'num_proxy_leaders':
                    input.num_proxy_leaders,
                'num_acceptor_groups':
                    input.num_acceptor_groups,
                'num_acceptors_per_group':
                    input.num_acceptors_per_group,
                'num_replicas':
                    input.num_replicas,
                'leader_flush_every_n':
                    input.leader_options.flush_phase2as_every_n,
                'proxy_leader_flush_every_n':
                    input.proxy_leader_options.flush_phase2as_every_n,
                'workload':
                    input.workload,
                'write.latency.median_ms': \
                    f'{output.write_output.latency.median_ms:.6}',
                'write.start_throughput_1s.p90': \
                    f'{output.write_output.start_throughput_1s.p90:.8}',
                'read.latency.median_ms': \
                    f'{output.read_output.latency.median_ms:.6}',
                'read.start_throughput_1s.p90': \
                    f'{output.read_output.start_throughput_1s.p90:.8}',
            })

    suite = Suite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'batched_read_scale') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
