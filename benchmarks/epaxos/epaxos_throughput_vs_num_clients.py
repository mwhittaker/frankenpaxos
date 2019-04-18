from .epaxos import *


def _main(args) -> None:
    inputs = [
        Input(
            net_name='SingleSwitchNet',
            f=1,
            num_clients=num_clients,
            num_threads_per_client=num_threads_per_client,
            duration_seconds=15,
            conflict_rate=conflict_rate,
            client_lag_seconds=5,
            client_repropose_period_seconds=client_repropose_period_seconds,
        )
        for client_repropose_period_seconds in [0.01, 0.1, 1, 10]
        for num_threads_per_client in range(1, 5)
        for conflict_rate in [0, 25, 50, 75, 100]
        for num_clients in [1, 2]
    ] * 2

    def make_net(input) -> EPaxosNet:
        return SingleSwitchNet(f=input.f, num_clients=input.num_clients)

    run_suite(args, inputs, make_net)


if __name__ == '__main__':
    _main(get_parser().parse_args())
