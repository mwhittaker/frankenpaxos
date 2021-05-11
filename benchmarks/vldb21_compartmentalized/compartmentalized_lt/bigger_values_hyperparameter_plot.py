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


ALPHA = 1
MARKERS = itertools.cycle(['o', '*', '^', 's', 'P', 'x', '1'])


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

    grouped = df.groupby([
        'num_replicas',
        'num_proxy_leaders',
        'num_acceptor_groups',
        'num_acceptors_per_group',
        'workload.write_size_mean',
        'leader_options.flush_phase2as_every_n'
    ])
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

    # Replace -1's with 0's.
    def replace_with_zero(df, label):
        df[label] = df[label].apply(lambda x: x if x > 0 else 0)
    replace_with_zero(df, 'write_output.start_throughput_1s.p90')
    replace_with_zero(df, 'read_output.start_throughput_1s.p90')
    replace_with_zero(df, 'write_output.latency.median_ms')
    replace_with_zero(df, 'read_output.latency.median_ms')

    # Abbreviate fields.
    df['throughput'] = df['write_output.start_throughput_1s.p90']
    df['latency'] = df['write_output.latency.median_ms']

    num_figs = 12
    fig, axes = plt.subplots(num_figs, 1, figsize=(6.4, 4.8 * num_figs * 1.5))
    axes_iter = iter(axes)

    for value_size in [100, 1000]:
        for flush_every_n in [10, 5, 20]:
            for num_acceptor_groups in [2, 3]:
                filtered = df[
                    (df['workload.write_size_mean'] == value_size) &
                    (df['leader_options.flush_phase2as_every_n'] == flush_every_n) &
                    (df['num_acceptor_groups'] == num_acceptor_groups)
                ]
                title = (
                    f'{value_size} byte values; '
                    f'flush every {flush_every_n}; '
                    f'{num_acceptor_groups} acceptor groups'
                )
                plot_lt(filtered, next(axes_iter), title)

    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        help='results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='bigger_values_compartmentalized_lt_hyperparameter.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
