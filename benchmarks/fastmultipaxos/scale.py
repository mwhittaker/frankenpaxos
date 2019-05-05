from .fastmultipaxos import *


def _main(args) -> None:
    inputs = [
        Input(
            net_name = 'SingleSwitchNet',
            f = 1,
            num_client_procs = num_client_procs,
            num_clients_per_proc = num_clients_per_proc,
            round_system_type = RoundSystemType.CLASSIC_ROUND_ROBIN.name,

            duration_seconds = 15,
            timeout_seconds = 30,
            client_lag_seconds = 5,
            command_size_bytes_mean = 0,
            command_size_bytes_stddev = 0,
            command_sleep_time_nanos_mean = 0,
            command_sleep_time_nanos_stddev = 0,
            profiled = args.profile,
            monitored = args.monitor,
            prometheus_scrape_interval_ms = 200,

            acceptor = AcceptorOptions()._replace(
                wait_period_ms = 0,
                wait_stagger_ms = 0,
            ),

            leader = LeaderOptions()._replace(
                thrifty_system = ThriftySystemType.NOT_THRIFTY,
                resend_phase1as_timer_period_ms = 1000,
                resend_phase2as_timer_period_ms = 1000,
                phase2a_max_buffer_size = 1,
                phase2a_buffer_flush_period_ms = 1000000000,
                value_chosen_max_buffer_size = 1,
                value_chosen_buffer_flush_period_ms = 1000000000,
            ),
            leader_log_level = "warn",

            client = ClientOptions()._replace(
                repropose_period_ms = 25,
            ),
        )

        for (num_client_procs, num_clients_per_proc) in
            [(1, 1)] +
            [(n, 10) for n in range(1, 15)]
    ] * 3

    def make_net(input) -> FastMultiPaxosNet:
        return SingleSwitchNet(
            f=input.f,
            num_client_procs=input.num_client_procs,
            rs_type = RoundSystemType[input.round_system_type]
        )

    run_suite(args, inputs, make_net, 'scale')


if __name__ == '__main__':
    _main(get_parser().parse_args())
