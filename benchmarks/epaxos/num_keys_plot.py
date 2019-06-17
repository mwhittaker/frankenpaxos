# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')

from .. import parser_util
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import textwrap


def wrapped(s: str, width: int = 60) -> str:
    return '\n'.join(textwrap.wrap(s, width))


def plot(df: pd.DataFrame, ax, column: str, pretty_column: str) -> None:
    stats = df.groupby('client_num_keys')[column].agg([np.mean, np.std])
    mean = stats['mean']
    std = stats['std'].fillna(0)

    x = np.arange(len(mean))
    ax.bar(x, mean, yerr=std)
    ax.set_xticks(x)
    ax.set_xticklabels(tuple(mean.index))

    ax.set_title(wrapped(f'{pretty_column} vs number of keys', 100))
    ax.set_xlabel('Number of keys')
    ax.set_ylabel(pretty_column)


def main(args) -> None:
    df = pd.read_csv(args.results)
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']

    num_plots = 4
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))

    plot(df, ax[0], 'median_latency_ms', 'Median latency (ms)')
    plot(df, ax[1], 'median_1_second_throughput',
                    'Median throughput (1 second windows)')
    plot(df, ax[2], 'p90_1_second_throughput',
                    'P90 throughput (1 second windows)')
    plot(df, ax[3], 'p95_1_second_throughput',
                    'P95 throughput (1 second windows)')

    fig.set_tight_layout(True)
    fig.savefig(args.output)
    print(f'Wrote plot to {args.output}.')


if __name__ == '__main__':
    parser = parser_util.get_plot_parser('epaxos_num_keys.pdf')
    main(parser.parse_args())
