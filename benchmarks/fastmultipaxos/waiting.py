from .fastmultipaxos import *
from typing import Collection


def _main(args) -> None:
    class WaitingFastMultiPaxosSuite(FastMultiPaxosSuite):
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
                    timeout_seconds = 60,
                    client_lag_seconds = 5,
                    command_size_bytes_mean = 0,
                    command_size_bytes_stddev = 0,
                    command_sleep_time_nanos_mean = 0,
                    command_sleep_time_nanos_stddev = 0,
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval_ms = 200,
                    acceptor = AcceptorOptions()._replace(
                        wait_period_ms = wait_period_ms,
                        wait_stagger_ms = wait_stagger_ms,
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
                    client = ClientOptions()._replace(
                        repropose_period_ms=25,
                    ),
                )
                for (num_client_procs, num_clients_per_proc) in
                    [(1, 1)] +
                    [(n, 9) for n in range(1, 6)]
                for (wait_period_ms, wait_stagger_ms) in [
                    (0.01, 0.), (0.05, 0.), (0.1, 0.), (1., 0.), (5., 0.)
                ]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'wait_period_ms': input.acceptor.wait_period_ms,
                'wait_stagger_ms': input.acceptor.wait_stagger_ms,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = WaitingFastMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'fastmultipaxos_waiting') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    _main(get_parser().parse_args())
