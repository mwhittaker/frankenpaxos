from .craq import *


def main(args) -> None:
    class SmokeCraqSuite(CraqSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = 1,
                    num_chain_nodes = num_chain_nodes,
                    client_jvm_heap_size = '100m',
                    chain_node_jvm_heap_size = '100m',
                    measurement_group_size = 1,
                    warmup_duration = datetime.timedelta(seconds=2),
                    warmup_timeout = datetime.timedelta(seconds=3),
                    warmup_sleep = datetime.timedelta(seconds=0),
                    duration = datetime.timedelta(seconds=2),
                    timeout = datetime.timedelta(seconds=3),
                    client_lag = datetime.timedelta(seconds=3),
                    workload_label = 'smoke',
                    workload = read_write_workload.PointSkewedReadWriteWorkload(
                        num_keys=10, read_fraction=read_fraction,
                        point_fraction=0.5, write_size_mean=1,
                        write_size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    chain_node_options = ChainNodeOptions(),
                    chain_node_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period = \
                            datetime.timedelta(seconds=1),
                        resend_read_request_period = \
                            datetime.timedelta(seconds=1),
                        flush_writes_every_n = 1,
                        flush_reads_every_n = 1,
                        batch_size = 1,
                    ),
                    client_log_level = args.log_level,
                )
                for num_chain_nodes in [2, 3, 4]
                for read_fraction in [0.0, 0.5, 1.0]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'num_chain_nodes': input.num_chain_nodes,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'workload': input.workload,
                'write.latency.median_ms': \
                    f'{output.write_output.latency.median_ms:.6}',
                'write.start_throughput_1s.p90': \
                    f'{output.write_output.start_throughput_1s.p90:.6}',
                'read.latency.median_ms': \
                    f'{output.read_output.latency.median_ms:.6}',
                'read.start_throughput_1s.p90': \
                    f'{output.read_output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeCraqSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'craq_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
