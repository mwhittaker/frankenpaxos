from .unanimousbpaxos import *


def main(args) -> None:
    class SmokeUnanimousBPaxosSuite(UnanimousBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = 1,
                    num_clients_per_proc = 1,
                    duration = datetime.timedelta(seconds=2),
                    timeout = datetime.timedelta(seconds=3),
                    client_lag = datetime.timedelta(seconds=0),
                    state_machine = 'Noop',
                    workload = workload.StringWorkload(size_mean=0, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    leader_options = LeaderOptions(),
                    leader_log_level = args.log_level,
                    leader_dependency_graph = 'Tarjan',
                    dep_service_node_options = DepServiceNodeOptions(),
                    dep_service_node_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    client_options = ClientOptions(),
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

    suite = SmokeUnanimousBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'unanimous_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
