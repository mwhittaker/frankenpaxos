from .mencius import *


def main(args) -> None:
    class SmokeMenciusSuite(MenciusSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 1,
                    # If we only run one client, then Mencius gets stuck.
                    num_clients_per_proc = 50,
                    num_batchers = 2,
                    num_leader_groups = 2,
                    num_leaders_per_group = 2,
                    num_proxy_leaders = 2,
                    num_acceptor_groups_per_leader_group = 2,
                    num_acceptors_per_group = 3,
                    num_replicas = 2,
                    num_proxy_replicas = 2,
                    distribution_scheme = DistributionScheme.HASH,
                    client_jvm_heap_size = '100m',
                    batcher_jvm_heap_size = '100m',
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
                    client_lag = datetime.timedelta(seconds=0),
                    state_machine = 'Noop',
                    workload = workload.StringWorkload(size_mean=1, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    batcher_options = BatcherOptions(
                        batch_size = 1,
                    ),
                    batcher_log_level = args.log_level,
                    leader_options = LeaderOptions(
                        send_high_watermark_every_n = 1000,
                        send_noop_range_if_lagging_by = 5000,
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
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'start_throughput_1s.p90': f'{output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeMenciusSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'mencius_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
