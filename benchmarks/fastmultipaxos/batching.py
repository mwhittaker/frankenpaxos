from .fastmultipaxos import *
from typing import Collection


def _main(args) -> None:
    class BatchingFastMultiPaxosSuite(FastMultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_clients_per_proc = num_clients_per_proc,
                    round_system_type = RoundSystemType.CLASSIC_ROUND_ROBIN,
                    duration_seconds = 20,
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
                        phase2a_max_buffer_size = phase2a_max_buffer_size,
                        phase2a_buffer_flush_period_ms = 100,
                        value_chosen_max_buffer_size =
                            value_chosen_max_buffer_size,
                        value_chosen_buffer_flush_period_ms = 1000,
                    ),
                    leader_log_level = "debug",
                    client = ClientOptions()._replace(
                        repropose_period_ms = 2000,
                    ),
                )
                for (num_client_procs, num_clients_per_proc) in
                    [(1, 1)] + [(n, 10) for n in [1, 2, 5, 7, 10]]
                for n in [num_client_procs * num_clients_per_proc]
                for phase2a_max_buffer_size in
                    [1 if x == 0 else x for x in range(0, n + 1, 10)]
                for value_chosen_max_buffer_size in [1000]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'phase2a_max_buffer_size':
                    input.leader.phase2a_max_buffer_size,
                'value_chosen_max_buffer_size':
                    input.leader.value_chosen_max_buffer_size,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = BatchingFastMultiPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'fastmultipaxos_batching') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    _main(get_parser().parse_args())
