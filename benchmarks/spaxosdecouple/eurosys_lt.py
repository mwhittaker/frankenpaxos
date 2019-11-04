from .spaxosdecouple import *


def main(args) -> None:
    class EuroSysLtSuperSPaxosDecoupleSuite(SPaxosDecoupleSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = num_clients_per_proc,
                    num_clients_per_proc = num_clients_per_proc,
                    num_batchers = num_batchers,
                    num_leaders = num_leaders,
                    num_proposers = num_proposers,
                    num_disseminator_groups = num_disseminator_groups,
                    num_proxy_leaders = num_proxy_leaders,
                    num_acceptor_groups = num_acceptor_groups,
                    num_replicas = num_replicas,
                    num_proxy_replicas = num_proxy_replicas,
                    distribution_scheme = \
                            DistributionScheme.HASH,
                    client_jvm_heap_size = '16g',
                    batcher_jvm_heap_size = '32g',
                    leader_jvm_heap_size = '32g',
                    proposer_jvm_heap_size = '32g',
                    disseminator_jvm_heap_size = '32g',
                    proxy_leader_jvm_heap_size = '32g',
                    acceptor_jvm_heap_size = '32g',
                    replica_jvm_heap_size = '32g',
                    proxy_replica_jvm_heap_size = '32g',
                    warmup_duration = datetime.timedelta(seconds=5),
                    warmup_timeout = datetime.timedelta(seconds=10),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=8),
                    timeout = datetime.timedelta(seconds=13),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'Noop',
                    workload = workload.StringWorkload(size_mean=1, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    batcher_options = BatcherOptions(
                        batch_size = batch_size,
                    ),
                    batcher_log_level = args.log_level,
                    proposer_options = ProposerOptions(
                        flush_forwards_every_n = \
                            proposer_flush_forwards_every_n,
                        flush_client_requests_every_n = \
                            proposer_flush_client_requests_every_n
                    ),
                    proposer_log_level = args.log_level,
                    disseminator_options = DisseminatorOptions(
                        flush_chosens_every_n = \
                            disseminator_flush_chosens_every_n,
                        flush_acknowledge_every_n = \
                            disseminator_flush_acknowledge_every_n,
                    ),
                    disseminator_log_level = args.log_level,
                    leader_options = LeaderOptions(
                        resend_phase1as_period = datetime.timedelta(seconds=60),
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
                        flush_value_chosens_every_n = proxy_leader_flush_every_n
                    ),
                    proxy_leader_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    replica_options = ReplicaOptions(
                        log_grow_size = 5000,
                        unsafe_dont_use_client_table = False,
                        send_chosen_watermark_every_n_entries = 100000,
                        recover_log_entry_min_period = \
                            datetime.timedelta(seconds=120),
                        recover_log_entry_max_period = \
                            datetime.timedelta(seconds=240),
                        unsafe_dont_recover = False,
                    ),
                    replica_log_level = args.log_level,
                    proxy_replica_options = ProxyReplicaOptions(
                        flush_every_n = proxy_replica_flush_every_n,
                    ),
                    proxy_replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period = \
                            datetime.timedelta(seconds=120),
                    ),
                    client_log_level = args.log_level,
                )

                for (
                    num_client_procs,
                    num_clients_per_proc,
                    num_batchers,
                    num_leaders,
                    num_proposers,
                    num_disseminator_groups,
                    num_proxy_leaders,
                    num_acceptor_groups,
                    num_replicas,
                    num_proxy_replicas,
                    batch_size,
                    leader_flush_every_n,
                    proxy_leader_flush_every_n,
                    proxy_replica_flush_every_n,
                    proposer_flush_forwards_every_n,
                    proposer_flush_client_requests_every_n,
                    disseminator_flush_chosens_every_n,
                    disseminator_flush_acknowledge_every_n
                ) in [
                    (10, 200, 0, 2, 13, 4, 14, 2, 2, 7, 0, 10, 1, 1, 1, 1, 1, 1),
                    (20, 200, 3, 2, 13, 4, 11, 2, 2, 7, 40, 1, 1, 1, 1, 1, 1, 1),
                ]
            ] * 3

        def summary(self, input: Input,
                    output: Output) -> str:
            return str({
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'num_batchers': input.num_batchers,
                'num_leaders': input.num_leaders,
                'num_proposers': input.num_proposers,
                'num_disseminator_groups': input.num_disseminator_groups,
                'num_proxy_leaders': input.num_proxy_leaders,
                'num_acceptor_groups': input.num_acceptor_groups,
                'num_replicas': input.num_replicas,
                'num_proxy_replicas': input.num_proxy_replicas,
                'batch_size': input.batcher_options.batch_size,
                'leader_flush_every_n': \
                        input.leader_options.flush_phase2as_every_n,
                'proxy_leader_flush_every_n': \
                        input.proxy_leader_options.flush_phase2as_every_n,
                'proxy_replica_flush_every_n': \
                        input.proxy_replica_options.flush_every_n,
                'proposer_flush_forwards_every_n': \
                        input.proposer_options.flush_forwards_every_n,
                'proposer_flush_client_requests_every_n': \
                        input.proposer_options.flush_client_requests_every_n,
                'disseminator_flush_chosens_every_n': \
                        input.disseminator_options.flush_chosens_every_n,
                'disseminator_flush_acknowledge_every_n': \
                        input.disseminator_options.flush_acknowledge_every_n,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.7}',
            })

    suite = EuroSysLtSuperSPaxosDecoupleSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'spaxosdecouple_eurosys_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
