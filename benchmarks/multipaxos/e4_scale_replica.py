from .multipaxos import *


def rf(num_writers: int, num_clients: int) -> int:
    return int((1 - (num_writers / num_clients)) * 100)


def main(args) -> None:
    class E4ScaleReplicaMultiPaxosSuite(MultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return ([
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
                    num_replicas = num_replicas,
                    num_proxy_replicas = 0,
                    distribution_scheme = DistributionScheme.HASH,
                    client_jvm_heap_size = '8g',
                    batcher_jvm_heap_size = '12g',
                    read_batcher_jvm_heap_size = '12g',
                    leader_jvm_heap_size = '12g',
                    proxy_leader_jvm_heap_size = '12g',
                    acceptor_jvm_heap_size = '12g',
                    replica_jvm_heap_size = '12g',
                    proxy_replica_jvm_heap_size = '12g',
                    measurement_group_size = measurement_group_size,
                    warmup_duration = datetime.timedelta(seconds=10),
                    warmup_timeout = datetime.timedelta(seconds=15),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=15),
                    timeout = datetime.timedelta(seconds=20),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    predetermined_read_fraction = predetermined_read_fraction,
                    workload_label = workload_label,
                    workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=0.0,
                        write_size_mean=16,
                        write_size_std=0),
                    read_workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=1.0,
                        write_size_mean=16,
                        write_size_std=0),
                    write_workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
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
                        resend_phase1as_period = datetime.timedelta(seconds=60),
                        flush_phase2as_every_n = 1,
                        noop_flush_period = noop_flush_period,
                        election_options = ElectionOptions(
                            ping_period = datetime.timedelta(seconds=60),
                            no_ping_timeout_min = \
                                datetime.timedelta(seconds=120),
                            no_ping_timeout_max = \
                                datetime.timedelta(seconds=240),
                        ),
                    ),
                    leader_log_level = args.log_level,
                    proxy_leader_options = ProxyLeaderOptions(),
                    proxy_leader_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    replica_options = ReplicaOptions(
                        log_grow_size = 5000,
                        unsafe_dont_use_client_table = False,
                        send_chosen_watermark_every_n_entries = 100,
                        recover_log_entry_min_period = \
                            datetime.timedelta(seconds=120),
                        recover_log_entry_max_period = \
                            datetime.timedelta(seconds=240),
                        unsafe_dont_recover = False,
                    ),
                    replica_log_level = args.log_level,
                    proxy_replica_options = ProxyReplicaOptions(
                        batch_flush = True,
                    ),
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

                for (label, predetermined_read_fraction,
                     num_replicas, num_client_procs, num_clients_per_proc) in [
                    # 25,000 writes.
                    ('25000', rf(113,  4*2*100),  2, 4*2,  100),
                    ('25000', rf(120, 7*2*100),  3, 5*2,  100),
                    ('25000', rf(143, 6*2*100),  4, 6*2,  100),
                    ('25000', rf(145, 7*2*100), 5, 7*2, 100),
                    ('25000', rf(150, 8*2*100), 6, 8*2, 100),

                    # 50,000 writes.
                    ('50000', rf(175, 3*2*100),  2, 3*2,  100),
                    ('50000', rf(175, 4*2*100),  3, 4*2,  100),
                    ('50000', rf(175, 5*2*100),  4, 5*2,  100),
                    ('50000', rf(227, 7*2*100),  5, 7*2,  100),
                    ('50000', rf(240, 8*2*100),  6, 8*2,  100),

                    # 75,000 writes.
                    ('75000', rf(310, 3*2*100), 2, 3*2, 100),
                    ('75000', rf(320, 4*2*100), 3, 4*2, 100),
                    ('75000', rf(330, 5*2*100), 4, 5*2, 100),
                    ('75000', rf(330, 6*2*100), 5, 6*2, 100),
                    ('75000', rf(330, 7*2*100), 6, 7*2, 100),

                    # 100,000 writes.
                    ('100000', rf(490, 5*100),  2, 5,  100),
                    ('100000', rf(490, 5*100),  3, 5,  100),
                    ('100000', rf(490, 5*104),  4, 5,  104),
                    ('100000', rf(495, 5*104),  5, 5,  104),
                    ('100000', rf(490, 5*106),  6, 5,  106),
                ]
                for workload_label in [label]
                for num_proxy_leaders in [10]
                for num_acceptor_groups in [5]
                for noop_flush_period in [datetime.timedelta(milliseconds=1)]
                for measurement_group_size in [100]
            ] * 5)[:]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'workload_label': input.workload_label,
                'read_fraction': input.predetermined_read_fraction,
                'num_replicas': input.num_replicas,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'write.latency.median_ms': \
                    f'{output.write_output.latency.median_ms:.6}',
                'write.start_throughput_1s.p90': \
                    f'{output.write_output.start_throughput_1s.p90:.8}',
                'read.latency.median_ms': \
                    f'{output.read_output.latency.median_ms:.6}',
                'read.start_throughput_1s.p90': \
                    f'{output.read_output.start_throughput_1s.p90:.8}',
            })

    suite = E4ScaleReplicaMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'multipaxos_e4_scale_replica') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
