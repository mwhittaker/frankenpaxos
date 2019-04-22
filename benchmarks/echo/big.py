from .echo import *

def _main(args) -> None:
    inputs = [
        Input(
            timeout_seconds=120,
            net_name='SingleSwitchNet',
            num_client_procs=num_client_procs,
            duration_seconds=duration_seconds,
            num_clients_per_proc=num_clients_per_proc,
            profiled=args.profile,
            monitored=args.monitor,
            prometheus_scrape_interval_ms=500,
        )
        for num_client_procs in range(1, 20)
        for duration_seconds in [15]
        for num_clients_per_proc in [1, 2, 3, 4, 5, 10, 25, 50, 100]
    ]

    def make_net(input) -> EchoNet:
        return SingleSwitchNet(num_client_procs=input.num_client_procs)

    run_suite(args, inputs, make_net)

if __name__ == '__main__':
    _main(get_parser().parse_args())
