# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')

import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import textwrap


def wrapped(s: str, width: int = 60) -> str:
    return '\n'.join(textwrap.wrap(s, width))


def plot(df: pd.DataFrame, ax, column: str, pretty_column: str) -> None:
    grouping_columns = ['round_system_type']
    for (name, group) in df.groupby(grouping_columns):
        stats = group.groupby('num_clients').agg([np.mean, np.std])
        mean = stats[column]['mean']
        std = stats[column]['std'].fillna(0)
        line = ax.plot(mean, '.-', label=name)[0]
        color = line.get_color()
        ax.fill_between(stats.index, mean - std, mean + std,
                        color=color, alpha=0.25)

    ax.set_title(wrapped(
        f'{pretty_column} for values of {grouping_columns}', 100))
    ax.set_xlabel('Number of clients')
    ax.set_ylabel(pretty_column)
    ax.grid()
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))


def main(args) -> None:
    df = pd.read_csv(args.results)
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']

    num_plots = 4
    fig, ax = plt.subplots(num_plots, 1, figsize=(2 * 6.4, num_plots * 4.8))

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


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'results',
        type=str,
        help='results.csv file'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='fastmultipaxos_scale.pdf',
        help='Output filename'
    )
    return parser


if __name__ == '__main__':
    main(get_parser().parse_args())
