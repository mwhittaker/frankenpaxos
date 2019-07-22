# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')

from .. import parser_util
from typing import Any, List
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import textwrap


def wrapped(s: str, width: int = 60) -> str:
    return '\n'.join(textwrap.wrap(s, width))


def sanitize(x: Any) -> Any:
    if isinstance(x, float):
        return f'{x:.6}'
    else:
        return x


def plot_vs_num_clients(df: pd.DataFrame,
                        ax: plt.Axes,
                        group_by: List[str],
                        title: str,
                        column: str,
                        pretty_column: str) -> None:
    for (name, group) in df.groupby(group_by):
        stats = group.groupby(['num_clients'])[column].agg([np.mean, np.std])
        mean = stats['mean']
        std = stats['std'].fillna(0)

        label = '\n'.join([f'{k}={sanitize(v)}'
                          for (k, v) in zip(group_by, name)])
        line = ax.semilogx(mean, '.-', label=label)[0]
        color = line.get_color()
        ax.fill_between(stats.index, mean - std, mean + std,
                        color=color, alpha=0.25)

    ax.set_title(wrapped(title, 100))
    ax.set_xlabel('Number of clients')
    ax.set_ylabel(pretty_column)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.grid()


def plot_latency_throughput(df: pd.DataFrame,
                            ax: plt.Axes,
                            line_group_by: List[str],
                            title: str) -> None:
    for (name, group) in df.groupby(line_group_by):
        label = '\n'.join([f'{k}={sanitize(v)}'
                          for (k, v) in zip(line_group_by, name)])
        grouped = group.groupby('num_clients')
        ax.plot(grouped['stop_throughput_1s.p90'].agg(np.mean),
                grouped['latency.median_ms'].agg(np.mean),
                '.-',
                label=label)

    ax.set_title(wrapped(title, 80))
    ax.set_xlabel('P90 throughput (1 second windows)')
    ax.set_ylabel('Median latency (ms)')
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.grid()


def main(args) -> None:
    df = pd.read_csv(args.results)
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']

    num_plots = 6
    scale = 2
    fig, ax = plt.subplots(num_plots, 1, figsize=(scale * 6.4, scale * num_plots * 4.8))
    ax_iter = iter(ax)
    line_group_by = [
        'f',
        'num_leaders',
        'replica_dependency_graph',
        'leader_options.thrifty_system',
    ]

    leader_df = df[
        (df['f'] == 1) &
        (df['replica_dependency_graph'] == 'Tarjan') &
        (df['leader_options.thrifty_system'] == 'Random')
    ]
    plot_vs_num_clients(leader_df, next(ax_iter), line_group_by,
         'Throughput', 'latency.median_ms', 'Median latency (ms)')
    plot_vs_num_clients(leader_df, next(ax_iter), line_group_by,
         'Latency', 'stop_throughput_1s.p90',
         'P90 throughput (1 second windows)')
    plot_latency_throughput(leader_df, next(ax_iter), line_group_by,
         'Latency vs Throughput')

    other_df = df[df['num_leaders'] == 6]
    plot_vs_num_clients(other_df, next(ax_iter), line_group_by,
         'Throughput', 'latency.median_ms', 'Median latency (ms)')
    plot_vs_num_clients(other_df, next(ax_iter), line_group_by,
         'Latency', 'stop_throughput_1s.p90',
         'P90 throughput (1 second windows)')
    plot_latency_throughput(other_df, next(ax_iter), line_group_by,
         'Latency vs Throughput')

    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


if __name__ == '__main__':
    parser = parser_util.get_plot_parser('simplebpaxos_scale.pdf')
    main(parser.parse_args())
