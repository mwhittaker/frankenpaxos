from .unanimousbpaxos import *
from typing import cast


def _main(args) -> None:
    class NumKeysUnanimousBPaxosSuite(UnanimousBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = f,
                    num_client_procs = 4,
                    num_clients_per_proc = 10,
                    duration = datetime.timedelta(seconds=20),
                    timeout = datetime.timedelta(seconds=45),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys = num_keys,
                        size_mean = 1,
                        size_std = 0,
                    ),
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
                for f in [1, 2]
                for num_keys in [1, 10, 100, 1000, 10000]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_keys':
                    cast(workload.UniformSingleKeyWorkload, input.workload)
                    .num_keys,
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = NumKeysUnanimousBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'unanimousbpaxos_num_keys') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    _main(get_parser().parse_args())
