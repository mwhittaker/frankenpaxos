from .epaxos import *


def main(args) -> None:
    class ScaleEPaxosSuite(EPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    net_name = 'SingleSwitchNet',
                    f = f,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = 10,
                    num_clients_per_proc = num_clients_per_proc,
                    warmup_duration = datetime.timedelta(seconds=5),
                    warmup_timeout = datetime.timedelta(seconds=10),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=20),
                    timeout = datetime.timedelta(seconds=60),
                    client_lag = datetime.timedelta(seconds=5),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    replica_options = ReplicaOptions(
                        resend_pre_accepts_timer_period = \
                            datetime.timedelta(seconds=60),
                        default_to_slow_path_timer_period = \
                            datetime.timedelta(seconds=60),
                        resend_accepts_timer_period = \
                            datetime.timedelta(seconds=60),
                        resend_prepares_timer_period = \
                            datetime.timedelta(seconds=60),
                        recover_instance_timer_min_period = \
                            datetime.timedelta(seconds=60),
                        recover_instance_timer_max_period = \
                            datetime.timedelta(seconds=120),
                        execute_graph_batch_size = 100,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1)
                    ),
                    replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(seconds=60),
                    ),
                    client_log_level = args.log_level,
                    client_num_keys = 1000000,
                )
                for f in [1, 2]
                for (num_client_procs, num_clients_per_proc) in
                    [(1, x) for x in [100, 1000, 10000, 50000, 100000]]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = ScaleEPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'epaxos_scale') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
