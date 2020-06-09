from .fasterpaxos import *


def main(args) -> None:
    class SmokeFasterPaxosSuite(FasterPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return ([
                Input(
                    f = f,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = num_clients_per_proc,
                    num_clients_per_proc = num_clients_per_proc,
                    num_servers = 2*f + 1,
                    client_jvm_heap_size = '8g',
                    server_jvm_heap_size = '12g',
                    measurement_group_size = 100,
                    warmup_duration = datetime.timedelta(seconds=10),
                    warmup_timeout = datetime.timedelta(seconds=15),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=15),
                    timeout = datetime.timedelta(seconds=20),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys=100, size_mean=16, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    server_options = ServerOptions(
                        ack_noops_with_commands = ack_noops_with_commands,
                        log_grow_size = 1000,
                        resend_phase1as_period = \
                            datetime.timedelta(seconds=1),
                        resend_phase2a_anys_period = \
                            datetime.timedelta(seconds=1),
                        use_f1_optimization = use_f1_optimization,
                        recover_log_entry_min_period = \
                            datetime.timedelta(seconds=5),
                        recover_log_entry_max_period = \
                            datetime.timedelta(seconds=10),
                        leader_change_entry_min_period = \
                            datetime.timedelta(seconds=5),
                        leader_change_entry_max_period = \
                            datetime.timedelta(seconds=10),
                        unsafe_dont_recover = False,
                        heartbeat_options = HeartbeatOptions(
                            fail_period = datetime.timedelta(seconds=1),
                            success_period = datetime.timedelta(seconds=2),
                            num_retries = 3,
                            network_delay_alpha = 0.9,
                        ),
                    ),
                    server_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period = \
                            datetime.timedelta(seconds=1),
                    ),
                    client_log_level = args.log_level,
                )

                for f in [1]
                for (ack_noops_with_commands, use_f1_optimization) in [
                    (False, False),
                    (False, True),
                    (True, False),
                ]
                for (num_client_procs, num_clients_per_proc) in [
                    (1, 1),
                    (1, 10),
                    (1, 25),
                    (1, 50),
                    (1, 75),
                    (1, 100),
                    (2, 100),
                    (3, 100),
                    (4, 100),
                    (5, 100),
                ]
            ] * 3)[:]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': \
                    f'{output.output.latency.median_ms:.6}',
                'start_throughput_1s.p90': \
                    f'{output.output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeFasterPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'fasterpaxos_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
