from .epaxos import *

def _main(args) -> None:
    with SuiteDirectory(args.suite_directory, 'epaxos') as suite:
        print(f'Running benchmark suite in {suite.path}.')
        suite.write_dict('args.json', vars(args))
        results_file = suite.create_file('results.csv')
        results_writer = csv.writer(results_file)
        results_writer.writerow(Input._fields + Output._fields)

        inputs = [
            Input(net_name='SingleSwitchNet',
                f=1,
                num_clients=1,
                num_threads_per_client=num_threads_per_client,
                duration_seconds=15,
                conflict_rate=conflict_rate,
                client_lag_seconds=5,
                client_repropose_period_seconds=client_repropose_period_seconds,
            )
            for client_repropose_period_seconds in [0.01, 0.1, 1, 10]
            for num_threads_per_client in range(1, 5)
            for conflict_rate in [0, 25, 50, 75, 100]
        ] * 3
        for input in tqdm(inputs):
            with suite.benchmark_directory() as bench:
                with SingleSwitchNet(
                        f=input.f,
                        num_clients=input.num_clients,
                     ) as net:
                    bench.write_string('input.txt', str(input))
                    bench.write_dict('input.json', input._asdict())
                    output = run_benchmark(bench, args, input, net)
                    row = [str(x) for x in list(input) + list(output)]
                    results_writer.writerow(row)
                    results_file.flush()

if __name__ == '__main__':
    _main(get_parser().parse_args())
