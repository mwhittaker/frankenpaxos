from .epaxos import *


def _main(args) -> None:
    inputs = [
        Input(
            net_name = 'SingleSwitchNet',
            f = 1,
            num_client_procs = num_client_procs,
            num_clients_per_proc = num_clients_per_proc,
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
            client_num_keys = 1000,
        )
        for (num_client_procs, num_clients_per_proc) in
            [(1, 1), (1, 10), (2, 10), (3, 10), (4, 10)]
    ] * 3

    def make_net(input) -> EPaxosNet:
        return SingleSwitchNet(f=input.f,
                               num_client_procs=input.num_client_procs)

    run_suite(args, inputs, make_net, 'scale')


if __name__ == '__main__':
    _main(get_parser().parse_args())
