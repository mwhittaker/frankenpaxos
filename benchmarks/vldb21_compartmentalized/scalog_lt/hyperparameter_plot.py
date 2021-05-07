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

    # Draw throughput.
    grouped = df.groupby(['num_shards',
                          'num_replicas',
                          'num_proxy_replicas',
                          'server_options.push_size',
                          'server_options.push_period',
                          'aggregator_options.num_shard_cuts_per_proposal',
                          'replica_options.unsafe_yolo_execution'])
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

        ax.grid()
        ax.set_title(title)
        ax.set_xlabel('Throughput (100,000 commands per second)')
        ax.set_ylabel('Latency\n(milliseconds)')
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))


def main(args) -> None:
    df = add_num_clients(pd.read_csv(args.results))

    # Abbreviate fields.
    df['throughput'] = df['output.start_throughput_1s.p90']
    df['latency'] = df['output.latency.median_ms']
    df['server_options.push_period'] = \
        pd.to_timedelta(df['server_options.push_period'])

    num_plots = 16
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, 4.8 * num_plots * 1.25))
    axes = iter(ax)

    for push_period_ms in [0.5, 1, 2, 4]:
        filter = (
            (df['server_options.push_period'] ==
             pd.Timedelta(milliseconds=push_period_ms)) &
            (df['workload_label'] == 'smoke')
        )
        plot_lt(df[filter], next(axes), f'push_period = {push_period_ms}ms')

    filter = (df['workload_label'] == 'sweep_v1')
    plot_lt(df[filter], next(axes), f'2 shards, 3 servers')

    filter = (df['workload_label'] == 'sweep_v2')
    plot_lt(df[filter], next(axes), f'One more shard, one more replica')

    filter = (df['workload_label'] == 'proxy_leaders_v1')
    plot_lt(df[filter], next(axes), f'With proxy leaders')

    filter = (df['workload_label'] == 'proxy_leaders_v2')
    plot_lt(df[filter], next(axes), f'One less replica')

    filter = (df['workload_label'] == 'smaller_push_period_v1')
    plot_lt(df[filter], next(axes), f'Smaller push periods')

    filter = (df['workload_label'] == 'yolo_v1')
    plot_lt(df[filter], next(axes), f'With yolo')

    filter = (df['workload_label'] == 'yolo_v2')
    plot_lt(df[filter], next(axes), f'yolo sweep')

    filter = (df['workload_label'] == 'push_size_v1')
    plot_lt(df[filter], next(axes), f'improved yolo + more sweep')

    filter = (
        (df['workload_label'] == 'big_sweep_v1') &
        (df['latency'] < 50) &
        (df['num_replicas'] == 3)
    )
    plot_lt(df[filter], next(axes), f'big yolo sweep (3 replicas)')

    filter = (
        (df['workload_label'] == 'big_sweep_v1') &
        (df['latency'] < 50) &
        (df['num_replicas'] == 2) &
        (df['num_shards'] <= 4)
    )
    plot_lt(df[filter], next(axes), f'big yolo sweep (2 replicas)')

    filter = (
        (df['workload_label'] == 'big_sweep_v1') &
        (df['latency'] < 50) &
        (df['num_replicas'] == 2) &
        (df['num_shards'] >= 5)
    )
    plot_lt(df[filter], next(axes), f'big yolo sweep (2 replicas)')

    filter = (
        (df['workload_label'] == 'batch_size_sweep_v1') &
        (df['latency'] < 20)
    )
    plot_lt(df[filter], next(axes), f'batch size sweep')

    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        help='results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='scalog_lt_hyperparameter.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
