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


def plot_throughput(df: pd.DataFrame, ax: plt.Axes) -> None:
    def outlier_throughput(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].mean() / 100000

    def outlier_throughput_std(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].std() / 100000

    # Draw throughput.
    grouped = df.groupby(('leader_options.flush_phase2as_every_n',
                          'num_replicas'))
    for (name, group) in grouped:
        print(f'## {name}')
        print(group[['throughput', 'latency']])

        by_num_proxy_leaders = group.groupby('num_proxy_leaders')
        throughput = by_num_proxy_leaders.apply(outlier_throughput).sort_index()
        std = by_num_proxy_leaders.apply(outlier_throughput_std).sort_index()
        print(f'throughput = {throughput}')
        print(f'std = {std}')
        print()
        line = ax.plot(throughput.index, throughput,
                       '-', marker = next(MARKERS), label=name, linewidth=1.5)[0]
        # Draw error bars.
        ax.fill_between(throughput.index,
                        throughput - std,
                        throughput + std,
                        color=line.get_color(),
                        alpha=0.3)


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

    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    plot_throughput(df, ax)

    ax.set_title('(leader flush every, num replicas)')
    ax.set_xlabel('Number of proxy leaders')
    ax.set_ylabel('Throughput\n(100,000 commands per second)')
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.grid()
    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        help='results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='compartmentalized_lt_hyperparameter.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
