# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 14}
matplotlib.rc('font', **font)

from typing import Any, List
import argparse
import datetime
import itertools
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re


MARKERS = itertools.cycle(['o', '*', '^', 's', 'P'])


def add_num_clients(df: pd.DataFrame) -> pd.DataFrame:
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    return df

def plot_lt(df: pd.DataFrame, ax: plt.Axes, title: str) -> None:
    def outlier_throughput(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].mean() / 100000

    def outlier_throughput_std(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].std() / 100000

    grouped = df.groupby(['server_options.flush_every_n',
                          'workload.size_mean'])
    for (name, group) in grouped:
        print(f'## {name}')
        print(group[['throughput', 'latency']])

        by_clients = group.groupby('num_clients')
        throughput = by_clients['throughput'].agg(np.mean).sort_index() / 1000
        throughput_std = by_clients['throughput'].agg(np.std).sort_index() / 1000
        latency = by_clients['latency'].agg(np.mean).sort_index()
        line = ax.plot(throughput, latency, '-', marker=next(MARKERS),
                       label=name, linewidth=2)[0]
        ax.fill_betweenx(latency,
                         throughput - throughput_std,
                         throughput + throughput_std,
                         color = line.get_color(),
                         alpha=0.25)

    ax.set_title(title)
    ax.set_xlabel('Throughput (100,000 commands per second)')
    ax.set_ylabel('Latency\n(milliseconds)')
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.grid(b=True)


def main(args) -> None:
    df = add_num_clients(pd.read_csv(args.results))

    # Abbreviate fields.
    df['throughput'] = df['start_throughput_1s.p90']
    df['latency'] = df['latency.median_ms']

    num_figures = 3
    fig, axes = plt.subplots(3, 1, figsize=(6.4, 4.8 * num_figures * 1.5))
    axes_iter = iter(axes)

    filtered = df[df['workload.size_mean'] == 16]
    plot_lt(filtered, next(axes_iter), '16 byte values')

    filtered = df[df['workload.size_mean'] == 100]
    plot_lt(filtered, next(axes_iter), '100 byte values')

    filtered = df[df['workload.size_mean'] == 1000]
    plot_lt(filtered, next(axes_iter), '1000 byte values')

    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        help='results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='unreplicated_lt_hyperparameter.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
