from .epaxos import *
from typing import cast


def main(args) -> None:
    class NumKeysEPaxosSuite(EPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = f,
                    num_client_procs = 4,
                    num_warmup_clients_per_proc = 10,
                    num_clients_per_proc = 10,
                    warmup_duration = datetime.timedelta(seconds=5),
                    warmup_timeout = datetime.timedelta(seconds=10),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=20),
                    timeout = datetime.timedelta(seconds=60),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    workload = workload.UniformSingleKeyWorkload(
                        num_keys=num_keys,
                        size_mean=1,
                        size_std=0,
                    ),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    replica_options = ReplicaOptions(),
                    replica_log_level = args.log_level,
                    replica_dependency_graph = "Tarjan",
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(milliseconds=500),
                    ),
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
                'throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = NumKeysEPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'epaxos_num_keys') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
