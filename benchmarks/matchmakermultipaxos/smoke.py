from .matchmakermultipaxos import *


def main(args) -> None:
    class SmokeMatchmakerMultiPaxosSuite(MatchmakerMultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = f,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = 1,
                    num_leaders = f + 1,
                    num_matchmakers = 2*f+1,
                    num_reconfigurers = f+1,
                    num_acceptors = 2*f+1,
                    num_replicas = 2*f+1,
                    client_jvm_heap_size = '100m',
                    leader_jvm_heap_size = '100m',
                    matchmaker_jvm_heap_size = '100m',
                    reconfigurer_jvm_heap_size = '100m',
                    acceptor_jvm_heap_size = '100m',
                    replica_jvm_heap_size = '100m',
                    driver_jvm_heap_size = '100m',
                    warmup_duration = datetime.timedelta(seconds=2),
                    warmup_timeout = datetime.timedelta(seconds=3),
                    warmup_sleep = datetime.timedelta(seconds=0),
                    duration = datetime.timedelta(seconds=2),
                    timeout = datetime.timedelta(seconds=3),
                    client_lag = datetime.timedelta(seconds=3),
                    state_machine = 'Noop',
                    workload = workload.StringWorkload(size_mean=1, size_std=0),
                    driver_workload = driver_workload.DoNothing(),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    leader_options = LeaderOptions(
                        resend_match_requests_period = \
                            datetime.timedelta(seconds=60),
                        resend_reconfigure_period = \
                            datetime.timedelta(seconds=60),
                        resend_phase1as_period = \
                            datetime.timedelta(seconds=60),
                        resend_phase2as_period = \
                            datetime.timedelta(seconds=60),
                        resend_executed_watermark_requests_period = \
                            datetime.timedelta(seconds=60),
                        resend_persisted_period = \
                            datetime.timedelta(seconds=60),
                        resend_garbage_collects_period = \
                            datetime.timedelta(seconds=60),
                        send_chosen_watermark_every_n = 100,
                        stutter = 1000,
                        election_options = ElectionOptions(
                            ping_period = datetime.timedelta(seconds=60),
                            no_ping_timeout_min = \
                                datetime.timedelta(seconds=120),
                            no_ping_timeout_max = \
                                datetime.timedelta(seconds=240),
                        ),
                    ),
                    leader_log_level = args.log_level,
                    matchmaker_options = MatchmakerOptions(),
                    matchmaker_log_level = args.log_level,
                    reconfigurer_options = ReconfigurerOptions(
                        resend_stops_period = \
                            datetime.timedelta(seconds=60),
                        resend_bootstraps_period = \
                            datetime.timedelta(seconds=60),
                        resend_match_phase1as_period = \
                            datetime.timedelta(seconds=60),
                        resend_match_phase2as_period = \
                            datetime.timedelta(seconds=60),
                    ),
                    reconfigurer_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    replica_options = ReplicaOptions(
                        log_grow_size = 5000,
                        unsafe_dont_use_client_table = False,
                        recover_log_entry_min_period = \
                            datetime.timedelta(seconds=120),
                        recover_log_entry_max_period = \
                            datetime.timedelta(seconds=240),
                        unsafe_dont_recover = False,
                    ),
                    replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period = \
                            datetime.timedelta(seconds=120),
                    ),
                    client_log_level = args.log_level,
                    driver_log_level = args.log_level,
                )
                for f in [1, 2]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = SmokeMatchmakerMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'matchmaker_multipaxos_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
