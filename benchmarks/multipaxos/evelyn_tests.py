from .multipaxos import *


def main(args) -> None:
    class EvelynTestsMultiPaxosSuite(MultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = 5,
                    num_warmup_clients_per_proc = 40,
                    num_clients_per_proc = 40,
                    num_batchers = 0,
                    num_leaders = 2,
                    num_proxy_leaders = 5,
                    num_acceptor_groups = num_acceptor_groups,
                    num_replicas = num_replicas,
                    num_proxy_replicas = 4,
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
                    duration = datetime.timedelta(seconds=8),
                    timeout = datetime.timedelta(seconds=10),
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
                    ),
                    client_log_level = args.log_level,
                )
                for num_acceptor_groups in [3]
                for num_replicas in [4]
                for num_keys in [100]
                for read_fraction in [0.9]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_batchers': input.num_batchers,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'write.latency.median_ms': \
                    f'{output.write_output.latency.median_ms:.6}',
                'write.stop_throughput_1s.p90': \
                    f'{output.write_output.stop_throughput_1s.p90:.6}',
                'read.latency.median_ms': \
                    f'{output.read_output.latency.median_ms:.6}',
                'read.stop_throughput_1s.p90': \
                    f'{output.read_output.stop_throughput_1s.p90:.6}',
            })

    suite = EvelynTestsMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'multipaxos_evelyn_tests') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
