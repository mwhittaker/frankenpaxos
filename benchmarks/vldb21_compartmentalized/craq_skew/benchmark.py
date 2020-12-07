from ...craq.craq import *


def main(args) -> None:
    class SmokeCraqSuite(CraqSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = num_clients_per_proc,
                    num_clients_per_proc = num_clients_per_proc,
                    num_chain_nodes = num_chain_nodes,
                    client_jvm_heap_size = '8g',
                    chain_node_jvm_heap_size = '12g',
                    measurement_group_size = 10,
                    warmup_duration = datetime.timedelta(seconds=10),
                    warmup_timeout = datetime.timedelta(seconds=15),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=15),
                    timeout = datetime.timedelta(seconds=20),
                    client_lag = datetime.timedelta(seconds=5),
                    workload_label = workload_label,
                    workload = read_write_workload.PointSkewedReadWriteWorkload(
                        num_keys=num_keys,
                        read_fraction=read_fraction,
                        point_fraction=point_fraction,
                        write_size_mean=16,
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

                # - 3 clients are needed to get a peak write-only throughput of
                #   about 80,000 comands per second with 3 chain nodes. This is
                #   roughly half of what we're getting with MultiPaxos, but it
                #   makes sense since every node has to touch 4 messages per
                #   command.
                # - 20 - 30 clients are needed to get a peak read-only
                #   throughput of about 400,000 comands per second with 3 chain
                #   nodes. This is mroe than Compartmentalized MultiPaxos which
                #   makes sense since a Craq read (in the happy case) goes only
                #   to a single chain node and back.
                for workload_label in ['craq_skew_1']
                for (
                    num_chain_nodes,      # 0
                    num_client_procs,     # 1
                    num_clients_per_proc, # 2
                    num_keys,             # 3
                    read_fraction,        # 4
                    point_fraction,       # 5
                ) in [
                    # 0  1    2       3     4      5
                    ( 6, 5, 100, 100000, 0.95, 0.000),
                    ( 6, 5, 100, 100000, 0.95, 0.125),
                    ( 6, 5, 100, 100000, 0.95, 0.250),
                    ( 6, 5, 100, 100000, 0.95, 0.375),
                    ( 6, 5, 100, 100000, 0.95, 0.500),
                    ( 6, 5, 100, 100000, 0.95, 0.625),
                    ( 6, 5, 100, 100000, 0.95, 0.750),
                    ( 6, 5, 100, 100000, 0.95, 0.875),
                    ( 6, 5, 100, 100000, 0.95, 1.000),
                ]
            ] * 5

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
                    f'{output.write_output.start_throughput_1s.p90:.7}',
                'read.latency.median_ms': \
                    f'{output.read_output.latency.median_ms:.6}',
                'read.start_throughput_1s.p90': \
                    f'{output.read_output.start_throughput_1s.p90:.7}',
            })

    suite = SmokeCraqSuite()
    with benchmark.SuiteDirectory(args.suite_directory, 'craq_skew') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
