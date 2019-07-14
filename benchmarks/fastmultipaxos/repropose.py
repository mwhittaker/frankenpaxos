from .fastmultipaxos import *
from typing import Collection

def _main(args) -> None:
    class ReproposeFastMultiPaxosSuite(FastMultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_clients_per_proc = num_clients_per_proc,
                    round_system_type = RoundSystemType.MIXED_ROUND_ROBIN,
                    duration_seconds = 20,
                    timeout_seconds = 30,
                    client_lag_seconds = 5,
                    command_size_bytes_mean = 0,
                    command_size_bytes_stddev = 0,
                    command_sleep_time_nanos_mean = 0,
                    command_sleep_time_nanos_stddev = 0,
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval_ms = 200,
                    acceptor = AcceptorOptions()._replace(
                        wait_period_ms = 0,
                        wait_stagger_ms = 0,
                    ),
                    leader = LeaderOptions()._replace(
                        thrifty_system = ThriftySystemType.NOT_THRIFTY,
                        resend_phase1as_timer_period_ms = 1000,
                        resend_phase2as_timer_period_ms = 1000,
                        phase2a_max_buffer_size = 1,
                        phase2a_buffer_flush_period_ms = 1000000000,
                        value_chosen_max_buffer_size = 1,
                        value_chosen_buffer_flush_period_ms = 1000000000,
                    ),
                    leader_log_level = "warn",
                    client = ClientOptions()._replace(
                        repropose_period_ms=repropose_period_ms,
                    ),
                )
                for (num_client_procs, num_clients_per_proc) in
                    [(1, 1)] + [(n, 10) for n in [1, 3, 6, 10]]
                for repropose_period_ms in [0.5, 2.5, 10, 25, 50]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = ReproposeFastMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'fastmultipaxos_repropose') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    _main(get_parser().parse_args())
