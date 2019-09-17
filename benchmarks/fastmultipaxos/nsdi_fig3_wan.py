from .fastmultipaxos import *
from typing import Collection


def _main(args) -> None:
    class NsdiFig3WanFastMultiPaxosSuite(FastMultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 50,
                    num_clients_per_proc = num_clients_per_proc,
                    round_system_type = RoundSystemType.CLASSIC_ROUND_ROBIN,
                    warmup_duration = datetime.timedelta(seconds=15),
                    warmup_timeout = datetime.timedelta(seconds=20),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration_seconds = 25,
                    timeout_seconds = 30,
                    client_lag_seconds = 5,
                    state_machine = 'KeyValueStore',
                    workload = workload.BernoulliSingleKeyWorkload(
                        conflict_rate = 0.0,
                        size_mean = 8,
                        size_std = 0,
                    ),
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
                for (num_client_procs, num_clients_per_proc) in [(1, 1)]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = NsdiFig3WanFastMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'fastmultipaxos_nsdi_fig3_wan') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    _main(get_parser().parse_args())
