from .horizontal import *
from . import driver_workload


def main(args) -> None:
    class LtHorizontalSuite(HorizontalSuite):
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
                    client_jvm_heap_size = '8g',
                    leader_jvm_heap_size = '12g',
                    acceptor_jvm_heap_size = '12g',
                    replica_jvm_heap_size = '12g',
                    driver_jvm_heap_size = '12g',
                    measurement_group_size = 1,
                    warmup_duration = datetime.timedelta(seconds=10),
                    warmup_timeout = datetime.timedelta(seconds=15),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=15),
                    timeout = datetime.timedelta(seconds=20),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'Noop',
                    workload = workload.StringWorkload(size_mean=1, size_std=0),
                    driver_workload = driver_workload.DoNothing(),
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
                for alpha in [100 * 1000]
                # for alpha in [1, 10, 50, 100, 250]
                for (num_client_procs, num_clients_per_proc) in [
                    (1, 1),
                    (1, 2),
                    (1, 3),
                    (1, 4),
                    (1, 5),
                    (1, 10),
                    (1, 20),
                    (1, 30),
                    (1, 40),
                    (1, 50),
                    (1, 100),
                    (2, 100),
                    (3, 100),
                    (4, 100),
                    (5, 100),
                ]
                # for (num_client_procs, num_clients_per_proc) in [
                #     (1, 1),
                #     (1, 10),
                #     (1, 25),
                #     (1, 50),
                #     (1, 100),
                #     (2, 100),
                # ]
            ] * 1)[:]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'alpha': input.leader_options.alpha,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'latency.p99_ms': f'{output.latency.p99_ms:.6}',
                'start_throughput_1s.p90': f'{output.start_throughput_1s.p90:.6}',
            })

    suite = LtHorizontalSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'horizontal_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
