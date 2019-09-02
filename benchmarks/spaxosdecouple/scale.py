from .spaxosdecouple import *
from typing import Collection


def _main(args) -> None:
    class ScaleSPaxosDecoupleSuite(SPaxosDecoupleSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_clients_per_proc = num_clients_per_proc,
                    round_system_type = RoundSystemType.CLASSIC_ROUND_ROBIN,
                    duration_seconds = 15,
                    timeout_seconds = 30,
                    client_lag_seconds = 5,
                    state_machine = 'Register',
                    workload = workload.StringWorkload(10, 0),
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
                        repropose_period_ms = 25,
                    ),
                    proposer = ProposerOptions(),
                    executor = ExecutorOptions()
                )
                for (num_client_procs, num_clients_per_proc) in
                    [(1, 1)] + [(n, 10) for n in range(1, 6)]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = ScaleSPaxosDecoupleSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'spaxos_decouple_scale') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    _main(get_parser().parse_args())
