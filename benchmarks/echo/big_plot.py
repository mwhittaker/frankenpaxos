# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')

from .. import parser_util
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd


def plot_latency(df: pd.DataFrame, ax) -> None:
    grouped = df.groupby('num_clients').agg([np.mean, np.std])
    columns = {
        'latency.mean_ms': 'mean',
        'latency.median_ms': 'median',
        'latency.p90_ms': 'P90',
        'latency.p95_ms': 'P95',
        'latency.p99_ms': 'P99',
    }
    for column in columns:
        mean = grouped[column]['mean']
        std = grouped[column]['std']
        line = ax.plot(mean, '.-', label=columns[column])[0]
        color = line.get_color()
        ax.fill_between(grouped.index, mean - std, mean + std, color=color,
                        alpha=0.25)

    ax.set_title('Latency')
    ax.set_xlabel('Number of clients')
    ax.set_ylabel('Latency (ms)')
    ax.grid()
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))


def plot_1_second_throughput(df: pd.DataFrame, ax) -> None:
    grouped = df.groupby('num_clients').agg([np.mean, np.std])
    columns = {
        'stop_throughput_1s.mean': 'mean',
        'stop_throughput_1s.median': 'median',
        'stop_throughput_1s.p90': 'P90',
        'stop_throughput_1s.p95': 'P95',
        'stop_throughput_1s.p99': 'P99',
    }
    for column in columns:
        mean = grouped[column]['mean']
        std = grouped[column]['std']
        line = ax.plot(mean, '.-', label=columns[column])[0]
        color = line.get_color()
        ax.fill_between(grouped.index, mean - std, mean + std, color=color,
                        alpha=0.25)

    ax.set_title('Throughput (1 second windows)')
    ax.set_xlabel('Number of clients')
    ax.set_ylabel('Throughput')
    ax.grid()
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))


def main(args) -> None:
    df = pd.read_csv(args.results_csv)
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']

    # See [1] for figure size defaults. We add an extra plot at the bottom for
    # a textual note.
    #
    # [1]: https://matplotlib.org/api/_as_gen/matplotlib.pyplot.figure.html
    num_plots = 2
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))

    plot_latency(df, ax[0])
    plot_1_second_throughput(df, ax[1])

    fig.set_tight_layout(True)
    fig.savefig(args.output)
    print(f'Wrote plot to {args.output}.')


if __name__ == '__main__':
    parser = parser_util.get_plot_parser('echo_big.pdf')
    main(parser.parse_args())
