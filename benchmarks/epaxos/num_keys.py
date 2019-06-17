from .epaxos import *


def _main(args) -> None:
    inputs = [
        Input(
            net_name = 'SingleSwitchNet',
            f = 2,
            num_client_procs = 4,
            num_clients_per_proc = 10,
            duration = datetime.timedelta(seconds=20),
            timeout = datetime.timedelta(seconds=60),
            client_lag = datetime.timedelta(seconds=5),
            profiled = args.profile,
            monitored = args.monitor,
            prometheus_scrape_interval = datetime.timedelta(milliseconds=200),
            replica_options = ReplicaOptions(),
            replica_log_level = 'debug',
            client_options = ClientOptions(
                repropose_period = datetime.timedelta(milliseconds=500),
            ),
            client_log_level = 'debug',
            client_num_keys = client_num_keys,
        )
        for client_num_keys in [1, 10, 50, 100, 1000, 10000]
    ] * 3

    def make_net(input) -> EPaxosNet:
        return SingleSwitchNet(f=input.f,
                               num_client_procs=input.num_client_procs)

    run_suite(args, inputs, make_net, 'num_keys')


if __name__ == '__main__':
    _main(get_parser().parse_args())
