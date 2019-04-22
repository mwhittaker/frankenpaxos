from .fastmultipaxos import *


def _main(args) -> None:
    inputs = [
        Input(
            net_name='SingleSwitchNet',
            f=1,
            num_client_procs=num_client_procs,
            num_clients_per_proc=num_clients_per_proc,
            round_system_type=round_system_type,

            duration_seconds=20,
            timeout_seconds=90,
            client_lag_seconds=5,
            profiled=args.profile,
            monitored=args.monitor,
            prometheus_scrape_interval_ms=200,

            acceptor = AcceptorOptions()._replace(
                wait_period_ms = 0,
                wait_stagger_ms = 0,
            ),

            leader = LeaderOptions()._replace(
                thrifty_system = ThriftySystemType.NOT_THRIFTY,
            ),

            client = ClientOptions()._replace(
                repropose_period_ms=50,
            ),
        )
        for num_client_procs in range(1, 30)
        for num_clients_per_proc in range(1, 30)
        for round_system_type in [
            RoundSystemType.CLASSIC_ROUND_ROBIN.name,
            RoundSystemType.MIXED_ROUND_ROBIN.name,
        ]
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
