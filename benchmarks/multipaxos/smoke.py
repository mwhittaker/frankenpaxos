from .multipaxos import *


def main(args) -> None:
    class SmokeMultiPaxosSuite(MultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = 1,
                    num_batchers = num_batchers,
                    num_read_batchers = num_batchers,
                    num_leaders = 2,
                    num_proxy_leaders = 2,
                    num_acceptor_groups = 2,
                    num_replicas = 2,
                    num_proxy_replicas = 2,
                    distribution_scheme = DistributionScheme.HASH,
                    client_jvm_heap_size = '100m',
                    batcher_jvm_heap_size = '100m',
                    read_batcher_jvm_heap_size = '100m',
                    leader_jvm_heap_size = '100m',
                    proxy_leader_jvm_heap_size = '100m',
                    acceptor_jvm_heap_size = '100m',
                    replica_jvm_heap_size = '100m',
                    proxy_replica_jvm_heap_size = '100m',
                    warmup_duration = datetime.timedelta(seconds=2),
                    warmup_timeout = datetime.timedelta(seconds=3),
                    warmup_sleep = datetime.timedelta(seconds=0),
                    duration = datetime.timedelta(seconds=2),
                    timeout = datetime.timedelta(seconds=3),
                    client_lag = datetime.timedelta(seconds=3),
                    state_machine = 'KeyValueStore',
                    predetermined_read_fraction = -1,
                    workload = read_write_workload.UniformReadWriteWorkload(
                        num_keys=1, read_fraction=0.5, write_size_mean=1,
                        write_size_std=0),
                    read_workload = read_write_workload.UniformReadWriteWorkload(
                        num_keys=1, read_fraction=1.0, write_size_mean=1,
                        write_size_std=0),
                    write_workload = read_write_workload.UniformReadWriteWorkload(
                        num_keys=1, read_fraction=0.0, write_size_mean=1,
                        write_size_std=0),
                    read_consistency = 'eventual',
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    batcher_options = BatcherOptions(
                        batch_size = 1,
                    ),
                    batcher_log_level = args.log_level,
                    read_batcher_options = ReadBatcherOptions(
                        read_batching_scheme = "size,1,10s",
                        unsafe_read_at_first_slot = False,
                        unsafe_read_at_i = False,
                    ),
                    read_batcher_log_level = args.log_level,
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
                    proxy_replica_options = ProxyReplicaOptions(),
                    proxy_replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period = \
                            datetime.timedelta(seconds=120),
                    ),
                    client_log_level = args.log_level,
                )
                for num_batchers in [0, 2]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_batchers': input.num_batchers,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'write.latency.median_ms': \
                    f'{output.write_output.latency.median_ms:.6}',
                'write.start_throughput_1s.p90': \
                    f'{output.write_output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'multipaxos_smoke') as dir:
        suite.run_multithreaded_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
