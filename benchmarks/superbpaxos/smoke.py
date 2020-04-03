from ..simplebpaxos import simplebpaxos
from .superbpaxos import *


def main(args) -> None:
    class SmokeSuperBPaxosSuite(SuperBPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[simplebpaxos.Input]:
            return [
                simplebpaxos.Input(
                    f = 1,
                    num_client_procs = 1,
                    num_warmup_clients_per_proc = 1,
                    num_clients_per_proc = 1,
                    num_leaders = 3,
                    num_dep_service_nodes = 3,
                    num_acceptors = 3,
                    num_replicas = 3,
                    jvm_heap_size = '100m',
                    warmup_duration = datetime.timedelta(seconds=2),
                    warmup_timeout = datetime.timedelta(seconds=3),
                    warmup_sleep = datetime.timedelta(seconds=0),
                    duration = datetime.timedelta(seconds=2),
                    timeout = datetime.timedelta(seconds=3),
                    client_lag = datetime.timedelta(seconds=0),
                    state_machine = 'Noop',
                    workload = workload.StringWorkload(size_mean=0, size_std=0),
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    leader_options = simplebpaxos.LeaderOptions(
                        thrifty_system = 'NotThrifty',
                        resend_dependency_requests_timer_period = \
                            datetime.timedelta(seconds=600)
                    ),
                    leader_log_level = args.log_level,
                    proposer_options = simplebpaxos.ProposerOptions(
                        thrifty_system = 'Random',
                        resend_phase1as_timer_period = \
                            datetime.timedelta(seconds=600),
                        resend_phase2as_timer_period = \
                            datetime.timedelta(seconds=600),
                    ),
                    proposer_log_level = args.log_level,
                    dep_service_node_options = \
                        simplebpaxos.DepServiceNodeOptions(
                            top_k_dependencies = 1,
                        ),
                    dep_service_node_log_level = args.log_level,
                    acceptor_options = simplebpaxos.AcceptorOptions(),
                    acceptor_log_level = args.log_level,
                    replica_options = simplebpaxos.ReplicaOptions(
                        recover_vertex_timer_min_period = \
                            datetime.timedelta(seconds=600),
                        recover_vertex_timer_max_period = \
                            datetime.timedelta(seconds=1200),
                        execute_graph_batch_size = 1,
                        execute_graph_timer_period = \
                            datetime.timedelta(seconds=1),
                        num_blockers = 1,
                    ),
                    replica_zigzag_options = simplebpaxos.ZigzagOptions(
                        vertices_grow_size = 1000,
                        garbage_collect_every_n_commands = 1000,
                    ),
                    replica_log_level = args.log_level,
                    client_options = simplebpaxos.ClientOptions(
                        repropose_period = datetime.timedelta(seconds=600),
                    ),
                    client_log_level = args.log_level,
                )
            ]

        def summary(self, input: simplebpaxos.Input,
                    output: simplebpaxos.Output) -> str:
            return str({
                'f': input.f,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': f'{output.latency.median_ms:.6}',
                'start_throughput_1s.p90': f'{output.start_throughput_1s.p90:.6}',
            })

    suite = SmokeSuperBPaxosSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'superbpaxos_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
