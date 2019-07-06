from .simplebpaxos import *


def _main(args) -> None:
    class NumKeysSimpleBPaxosSuite(SimpleBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    net_name = 'SingleSwitchNet',
                    f = f,
                    num_client_procs = 4,
                    num_warmup_clients_per_proc = 10,
                    num_clients_per_proc = 10,
                    num_leaders = f + 1,
                    warmup_duration = datetime.timedelta(seconds=5),
                    warmup_timeout = datetime.timedelta(seconds=10),
                    duration = datetime.timedelta(seconds=20),
                    timeout = datetime.timedelta(seconds=45),
                    client_lag = datetime.timedelta(seconds=5),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    leader_options = LeaderOptions(),
                    leader_log_level = 'debug',
                    proposer_options = ProposerOptions(),
                    proposer_log_level = 'debug',
                    dep_service_node_options = DepServiceNodeOptions(),
                    dep_service_node_log_level = 'debug',
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = 'debug',
                    replica_options = ReplicaOptions(),
                    replica_log_level = 'debug',
                    client_options = ClientOptions(),
                    client_log_level = 'debug',
                    client_num_keys = client_num_keys,
                )
                for f in [1, 2]
                for client_num_keys in [1, 10, 100, 1000, 10000]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'client_num_keys': input.client_num_keys,
                'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}',
            })

    suite = NumKeysSimpleBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'simplebpaxos_num_keys') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    _main(get_parser().parse_args())
