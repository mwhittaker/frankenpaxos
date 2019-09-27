from .fastmultipaxos import *
from typing import Collection


def _main(args) -> None:
    class SmokeFastMultiPaxosSuite(FastMultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = 1,
                    round_system_type = RoundSystemType.CLASSIC_ROUND_ROBIN,
                    jvm_heap_size = '100m',
                    warmup_duration = datetime.timedelta(seconds=2),
                    warmup_timeout = datetime.timedelta(seconds=3),
                    warmup_sleep = datetime.timedelta(seconds=0),
                    duration_seconds = 2,
                    timeout_seconds = 3,
                    client_lag_seconds = 0,
                    state_machine = 'Noop',
                    workload = workload.StringWorkload(size_mean=0, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval_ms = 200,
                    acceptor = AcceptorOptions()._replace(
                        wait_period_ms = 0,
                        wait_stagger_ms = 0,
                    ),
                    acceptor_log_level = args.log_level,
                    leader = LeaderOptions()._replace(
                        thrifty_system = ThriftySystemType.RANDOM,
                        resend_phase1as_timer_period_ms = 600 * 1000,
                        resend_phase2as_timer_period_ms = 600 * 1000,
                        phase2a_max_buffer_size = 1,
                        phase2a_buffer_flush_period_ms = 600 * 1000,
                        value_chosen_max_buffer_size = 1,
                        value_chosen_buffer_flush_period_ms = 600 * 1000,
                        election = ElectionOptions(
                            ping_period_ms = 600 * 1000,
                            no_ping_timeout_min_ms = 1200 * 1000,
                            no_ping_timeout_max_ms = 1500 * 1000,
                            not_enough_votes_timeout_min_ms = 1200 * 1000,
                            not_enough_votes_timeout_max_ms = 1500 * 1000,
                        ),
                        heartbeat = HeartbeatOptions(
                            fail_period_ms = 600 * 1000,
                            success_period_ms = 1200 * 1000,
                            num_retries = 3,
                            network_delay_alpha = 0.9,
                        ),
                    ),
                    leader_log_level = args.log_level,
                    client = ClientOptions()._replace(
                        repropose_period_ms = 600 * 1000,
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
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = SmokeFastMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'fastmultipaxos_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    _main(get_parser().parse_args())
