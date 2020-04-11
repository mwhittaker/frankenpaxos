from .multipaxos import *


def main(args) -> None:
    class EvelynTestsMultiPaxosSuite(MultiPaxosSuite):
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
                    num_leaders = 2,
                    num_proxy_leaders = num_proxy_leaders,
                    num_acceptor_groups = num_acceptor_groups,
                    num_replicas = num_replicas,
                    num_proxy_replicas = num_proxy_replicas,
                    distribution_scheme = DistributionScheme.HASH,
                    client_jvm_heap_size = '12g',
                    batcher_jvm_heap_size = '12g',
                    leader_jvm_heap_size = '12g',
                    proxy_leader_jvm_heap_size = '12g',
                    acceptor_jvm_heap_size = '12g',
                    replica_jvm_heap_size = '12g',
                    proxy_replica_jvm_heap_size = '12g',
                    warmup_duration = datetime.timedelta(seconds=5),
                    warmup_timeout = datetime.timedelta(seconds=10),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=15),
                    timeout = datetime.timedelta(seconds=20),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    workload = read_write_workload.UniformReadWriteWorkload(
                        num_keys=num_keys, read_fraction=read_fraction,
                        write_size_mean=1, write_size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    batcher_options = BatcherOptions(
                        batch_size = 1,
                    ),
                    batcher_log_level = args.log_level,
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
                        unsafe_read_at_first_slot = unsafe_read_at_first_slot,
                        unsafe_read_at_i = unsafe_read_at_i,
                    ),
                    client_log_level = args.log_level,
                )
                for num_keys in [1]
                # for read_fraction in [0.9]
                for (unsafe_read_at_first_slot, unsafe_read_at_i) in
                    [(True, False)]
                    # [(False, False),
                    # (False, True),
                    # (True, False),]

                for (num_client_procs, num_clients_per_proc, read_fraction) in [
                    # (1, 100, 0.5),
                    # (2, 100, 1 - (100 / 200)),
                    (3, 100, 1 - (100 / 300)),
                    (4, 100, 1 - (100 / 400)),
                    (5, 100, 1 - (100 / 500)),
                    (6, 100, 1 - (100 / 600)),
                    (7, 100, 1 - (100 / 700)),
                    (8, 100, 1 - (100 / 800)),
                    (9, 100, 1 - (100 / 900)),
                    (10, 100, 1 - (100 / 1000)),
                    (11, 100, 1 - (100 / 1100)),
                    (12, 100, 1 - (100 / 1200)),
                    (13, 100, 1 - (100 / 1300)),
                    (14, 100, 1 - (100 / 1400)),
                    (15, 100, 1 - (100 / 1500)),
                ]
                for num_proxy_leaders in [2]
                for num_acceptor_groups in [3]
                for (num_replicas, num_proxy_replicas) in [(4, 2)]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_batchers': input.num_batchers,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'unsafe_read_at_i': \
                    input.client_options.unsafe_read_at_i,
                'unsafe_read_at_first_slot': \
                    input.client_options.unsafe_read_at_first_slot,
                'write.latency.median_ms': \
                    f'{output.write_output.latency.median_ms:.6}',
                'write.start_throughput_1s.p90': \
                    f'{output.write_output.start_throughput_1s.p90:.6}',
                'read.latency.median_ms': \
                    f'{output.read_output.latency.median_ms:.6}',
                'read.start_throughput_1s.p90': \
                    f'{output.read_output.start_throughput_1s.p90:.6}',
            })

    suite = EvelynTestsMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'multipaxos_evelyn_tests') as dir:
        suite.run_multithreaded_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
