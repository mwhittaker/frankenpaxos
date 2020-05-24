from .multipaxos import *


def main(args) -> None:
    class E3NoScaleFractionMultiPaxosSuite(MultiPaxosSuite):
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
                    num_proxy_replicas = num_proxy_replicas,
                    distribution_scheme = DistributionScheme.HASH,
                    client_jvm_heap_size = '12g',
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
                    predetermined_read_fraction = -1,
                    workload_label = workload_label,
                    workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=read_fraction,
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
                        resend_client_request_period = \
                            datetime.timedelta(seconds=120),
                        unsafe_read_at_first_slot = False,
                        unsafe_read_at_i = False,
                    ),
                    client_log_level = args.log_level,
                )

                for (read_fraction,
                     num_replicas, num_client_procs, num_clients_per_proc) in [
                    (0.0000, 2, 10, 50), # 1.00x
                    (0.1250, 2, 10, int(106 / 2)), # 1.06x
                    (0.2500, 2, 10, int(114 / 2)), # 1.14x
                    (0.3750, 2, 10, int(123 / 2)), # 1.23x
                    (0.5000, 2, 10, int(133 / 2)), # 1.33x
                    (0.5625, 2, 10, int(139 / 2)), # 1.39x
                    (0.6250, 2, 10, int(145 / 2)), # 1.45x
                    (0.6875, 2, 10, int(152 / 2)), # 1.52x
                    (0.7500, 2, 10, int(160 / 2)), # 1.60x
                    (0.8125, 2, 10, int(168 / 2)), # 1.68x
                    (0.8750, 2, 10, int(177 / 2)), # 1.77x
                    (0.9375, 2, 10, int(188 / 2)), # 1.88x
                    (1.0000, 2, 10, 100), # 2.00x

                    (0.0000, 4, 5, 100), # 1.00x
                    (0.1250, 4, 5, 110), # 1.10x
                    (0.2500, 4, 6, 100), # 1.23x
                    (0.3750, 4, 7, 100), # 1.39x
                    (0.5000, 4, 8, 100), # 1.60x
                    (0.5625, 4, 8, 150), # 1.73x
                    (0.6250, 4, 9, 100), # 1.88x
                    (0.6875, 4, 10, 100), # 2.06x
                    (0.7500, 4, 11, 100), # 2.28x
                    (0.8125, 4, 13, 100), # 2.56x
                    (0.8750, 4, 15, 100), # 2.90x
                    (0.9375, 4, 17, 100), # 3.36x
                    (1.0000, 4, 20, 100), # 4.00x

                    (0.0000, 6, 5, 100), # 1.00x
                    (0.1250, 6, 5, 112), # 1.12x
                    (0.2500, 6, 6, 100), # 1.26x
                    (0.3750, 6, 7, 100), # 1.45x
                    (0.5000, 6, 8, 100), # 1.71x
                    (0.5625, 6, 9, 100), # 1.88x
                    (0.6250, 6, 10, 100), # 2.08x
                    (0.6875, 6, 12, 100), # 2.34x
                    (0.7500, 6, 13, 100), # 2.66x
                    (0.8125, 6, 15, 100), # 3.09x
                    (0.8750, 6, 19, 100), # 3.69x
                    (0.9375, 6, 20, 112), # 4.50x
                    (1.0000, 6, 20, 150), # 6.00x
                ]
                for workload_label in [str(read_fraction)]
                for num_proxy_leaders in [10]
                for num_acceptor_groups in [5]
                for num_proxy_replicas in [num_replicas]
                for noop_flush_period in [datetime.timedelta(microseconds=500)]
                for measurement_group_size in [100]
            ] * 3)[:]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'workload_label': input.workload_label,
                'num_replicas': input.num_replicas,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'write.latency.median_ms': \
                    f'{output.write_output.latency.median_ms:.6}',
                'write.start_throughput_1s.p90': \
                    f'{output.write_output.start_throughput_1s.p90:.6}',
                'read.latency.median_ms': \
                    f'{output.read_output.latency.median_ms:.6}',
                'read.start_throughput_1s.p90': \
                    f'{output.read_output.start_throughput_1s.p90:.8}',
            })

    suite = E3NoScaleFractionMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'multipaxos_e3_no_scale_fraction') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
