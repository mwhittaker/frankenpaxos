from .echo import *

def _main(args) -> None:
    inputs = [
        Input(
            net_name='SingleSwitchNet',
            num_clients=num_clients,
            num_threads_per_client=num_threads_per_client,
            duration_seconds=duration_seconds,
            profiled=args.profile,
            prometheus_scrape_interval_ms=500,
        )
        for num_clients in range(1, 30)
        for num_threads_per_client in [1]
        for duration_seconds in [10]
    ]

    def make_net(input) -> EchoNet:
        return SingleSwitchNet(num_clients=input.num_clients)

    run_suite(args, inputs, make_net)

if __name__ == '__main__':
    _main(get_parser().parse_args())
