from .horizontal import *
from . import driver_workload


def main(args) -> None:
    class LeaderReconfigurationSuite(HorizontalSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return ([
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = num_clients_per_proc,
                    num_clients_per_proc = num_clients_per_proc,
                    num_leaders = 2,
                    num_acceptors = 6,
                    num_replicas = 3,
                    client_jvm_heap_size = '15g',
                    leader_jvm_heap_size = '15g',
                    acceptor_jvm_heap_size = '15g',
                    replica_jvm_heap_size = '15g',
                    driver_jvm_heap_size = '15g',
                    measurement_group_size = 1,
                    warmup_duration = datetime.timedelta(seconds=10),
                    warmup_timeout = datetime.timedelta(seconds=15),
                    warmup_sleep = datetime.timedelta(seconds=0),
                    duration = datetime.timedelta(seconds=55),
                    timeout = datetime.timedelta(seconds=60),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'Noop',
                    workload = workload.StringWorkload(size_mean=1, size_std=0),
                    driver_workload = \
                        driver_workload.LeaderReconfiguration(
                            reconfiguration_warmup_delay_ms = 10 * 1000,
                            reconfiguration_warmup_period_ms = 100,
                            reconfiguration_warmup_num = 100,
                            reconfiguration_delay_ms = 30 * 1000,
                            reconfiguration_period_ms = 1000,
                            reconfiguration_num = 10,
                            failure_delay_ms = 45 * 1000,
                            recover_delay_ms = 50 * 1000,
                        ),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    leader_options = LeaderOptions(
                        log_grow_size = 5000,
                        alpha = alpha,
                        resend_phase1as_period = \
                            datetime.timedelta(seconds=60),
                        election_options = ElectionOptions(
                            ping_period = datetime.timedelta(seconds=60),
                            no_ping_timeout_min = \
                                datetime.timedelta(seconds=120),
                            no_ping_timeout_max = \
                                datetime.timedelta(seconds=240),
                        ),
                    ),
                    leader_log_level = args.log_level,
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
                            datetime.timedelta(milliseconds=100),
                    ),
                    client_log_level = args.log_level,
                    driver_log_level = args.log_level,
                )
                for alpha in [10]
                for (num_client_procs, num_clients_per_proc) in [
                    (1, 1),
                    (4, 1),
                    (4, 2),
                ]
            ] * 1)[:]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num client procs': input.num_client_procs,
                'num clients per proc': input.num_clients_per_proc,
                'median latency': f'{output.latency.median_ms:.6}',
                'p99 latency': f'{output.latency.p99_ms:.6}',
                'throughput': f'{output.start_throughput_1s.p90:.6}',
            })

    suite = LeaderReconfigurationSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'horizontal_leader_reconfiguration') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
