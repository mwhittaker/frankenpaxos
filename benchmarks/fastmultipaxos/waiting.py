from .fastmultipaxos import *


def _main(args) -> None:
    inputs = [
        Input(
            net_name='SingleSwitchNet',
            f=1,
            num_client_procs=num_client_procs,
            num_clients_per_proc=num_clients_per_proc,
            round_system_type=RoundSystemType.MIXED_ROUND_ROBIN.name,

            duration_seconds=15,
            timeout_seconds=60,
            client_lag_seconds=5,
            profiled=args.profile,
            monitored=args.monitor,
            prometheus_scrape_interval_ms=200,

            acceptor = AcceptorOptions()._replace(
                wait_period_ms = wait_period_ms,
                wait_stagger_ms = wait_stagger_ms,
            ),

            leader = LeaderOptions()._replace(
                thrifty_system = ThriftySystemType.NOT_THRIFTY,
            ),

            client = ClientOptions()._replace(
                repropose_period_ms=repropose_period_ms,
            ),
        )
        for num_client_procs in range(1, 20, 3)
        for num_clients_per_proc in [1, 10]
        for (wait_period_ms, wait_stagger_ms) in [
            (0.01, 0.), (0.05, 0.), (0.1, 0.), (1., 0.), (5., 0.)
        ]
        for repropose_period_ms in [10]
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
