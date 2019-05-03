from .fastmultipaxos import *


def _main(args) -> None:
    inputs = [
        Input(
            # System-wide parameters.
            net_name = 'SingleSwitchNet',
            f = 1,
            num_client_procs = 1,
            num_clients_per_proc = 9,
            round_system_type = RoundSystemType.CLASSIC_ROUND_ROBIN.name,

            # Benchmark parameters.
            duration_seconds = 20,
            timeout_seconds = 60,
            client_lag_seconds = 5,
            command_size_bytes_mean = command_size_bytes_mean,
            command_size_bytes_stddev = 0,
            command_sleep_time_ms_mean = 0,
            command_sleep_time_ms_stddev = 0,
            profiled = args.profile,
            monitored = args.monitor,
            prometheus_scrape_interval_ms = 200,

            # Acceptor options.
            acceptor = AcceptorOptions()._replace(
                wait_period_ms = 0,
                wait_stagger_ms = 0,
            ),

            # Leader options.
            leader = LeaderOptions()._replace(
                thrifty_system = ThriftySystemType.NOT_THRIFTY,
                resend_phase1as_timer_period_ms = 1000,
                resend_phase2as_timer_period_ms = 1000,
                phase2a_max_buffer_size = 1,
                phase2a_buffer_flush_period_ms = 1000,
                value_chosen_max_buffer_size = 1,
                value_chosen_buffer_flush_period_ms = 1000,
            ),

            # Client options.
            client = ClientOptions()._replace(
                repropose_period_ms = 20000,
            ),
        )
        for command_size_bytes_mean in [1, 10, 100, 1000, 10000]
    ] * 3

    def make_net(input) -> FastMultiPaxosNet:
        return SingleSwitchNet(
            f=input.f,
            num_client_procs=input.num_client_procs,
            rs_type = RoundSystemType[input.round_system_type]
        )

    run_suite(args, inputs, make_net)


if __name__ == '__main__':
    _main(get_parser().parse_args())
