from .echo import *


def _main(args) -> None:
    class BigEchoSuite(EchoSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    duration_seconds=10,
                    timeout_seconds=20,
                    net_name='SingleSwitchNet',
                    num_client_procs=num_client_procs,
                    num_clients_per_proc=num_clients_per_proc,
                    profiled=args.profile,
                    monitored=args.monitor,
                    prometheus_scrape_interval_ms=500,
                )
                for (num_client_procs, num_clients_per_proc) in
                    [(1, 1)] + [(i, 10000) for i in [1, 2, 3, 4, 5]]
            ] * 3

        def make_net(self, args: Dict[Any, Any], input: Input) -> EchoNet:
            if args['address'] is not None:
                return RemoteEchoNet(args['address'],
                                     num_client_procs=input.num_client_procs)
            else:
                return SingleSwitchMininet(num_client_procs=
                                           input.num_client_procs)

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': output.latency.median_ms,
                'stop_throughput_1s.p90': output.stop_throughput_1s.p90,
            })

    suite = BigEchoSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'echo_big') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    _main(get_parser().parse_args())
