from .supermultipaxos import *


def main(args) -> None:
    class SmokeSuperMultiPaxosSuite(SuperMultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[multipaxos.Input]:
            return [
                multipaxos.Input(
                    f = 1,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = 1,
                    num_batchers = num_batchers,
                    num_leaders = 3,
                    num_proxy_leaders = 3,
                    num_acceptor_groups = 1,
                    num_replicas = 3,
                    num_proxy_replicas = 3,
                    distribution_scheme = \
                            multipaxos.DistributionScheme.COLOCATED,
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
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'Noop',
                    workload = workload.StringWorkload(size_mean=1, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    batcher_options = multipaxos.BatcherOptions(
                        batch_size = 1,
                    ),
                    batcher_log_level = args.log_level,
                    leader_options = multipaxos.LeaderOptions(
                        resend_phase1as_period = datetime.timedelta(seconds=60),
                        flush_phase2as_every_n = 1,
                        election_options = multipaxos.ElectionOptions(
                            ping_period = datetime.timedelta(seconds=60),
                            no_ping_timeout_min = \
                                datetime.timedelta(seconds=120),
                            no_ping_timeout_max = \
                                datetime.timedelta(seconds=240),
                        ),
                    ),
                    leader_log_level = args.log_level,
                    proxy_leader_options = multipaxos.ProxyLeaderOptions(),
                    proxy_leader_log_level = args.log_level,
                    acceptor_options = multipaxos.AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    replica_options = multipaxos.ReplicaOptions(
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
                    proxy_replica_options = multipaxos.ProxyReplicaOptions(),
                    proxy_replica_log_level = args.log_level,
                    client_options = multipaxos.ClientOptions(
                        resend_client_request_period = \
                            datetime.timedelta(seconds=120),
                    ),
                    client_log_level = args.log_level,
                )

                for num_batchers in [0, 3]
            ]

        def summary(self, input: multipaxos.Input,
                    output: multipaxos.Output) -> str:
            return str({
                'f': input.f,
                'num_batchers': input.num_batchers,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'start_throughput_1s.p90': f'{output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeSuperMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'super_multipaxos_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
