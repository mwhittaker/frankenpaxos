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
                    num_clients_per_proc = num_clients_per_proc,
                    duration = datetime.timedelta(seconds=20),
                    timeout = datetime.timedelta(seconds=60),
                    client_lag = datetime.timedelta(seconds=5),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    replica_options = ReplicaOptions(),
                    replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        repropose_period = datetime.timedelta(milliseconds=500),
                    ),
                    client_log_level = args.log_level,
                    client_num_keys = 1000,
                )
                for f in [1, 2]
                for (num_client_procs, num_clients_per_proc) in
                    [(1, 1), (1, 10), (2, 10), (3, 10), (4, 10)]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'output.throughput_1s.p90': f'{output.throughput_1s.p90:.6}',
            })

    suite = ScaleEPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'epaxos_scale') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
