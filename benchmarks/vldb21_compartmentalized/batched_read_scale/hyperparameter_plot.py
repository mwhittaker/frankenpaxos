# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 14}
matplotlib.rc('font', **font)

from typing import Any, List, Tuple
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


def plot(df: pd.DataFrame,
         ax: plt.Axes,
         grouping_columns: Tuple[str, ...],
         x_column: str,
         y_columns: List[str],
         title: str) -> None:
    def outlier_mean(c: str):
        def f(g: pd.DataFrame) -> float:
            cutoff = 0.5 * g[c].max()
            return g[g[c] >= cutoff][c].mean()
        return f

    def outlier_std(c: str):
        def f(g: pd.DataFrame):
            cutoff = 0.5 * g[c].max()
            return g[g[c] >= cutoff][c].std()
        return f

    grouped = df.groupby(grouping_columns)
    for (name, group) in grouped:
        name = [name] if len(grouping_columns) == 1 else name
        label = ','.join(f'{f}={x}' for (f, x) in zip(grouping_columns, name))
        by_acceptors = group.groupby(x_column)
        for c in y_columns:
            throughput = by_acceptors.apply(outlier_mean(c)).sort_index()
            std = by_acceptors.apply(outlier_std(c)).sort_index()
            line = ax.plot(throughput.index, throughput, '-',
                           marker = next(MARKERS), label=f'{c} {label}',
                           linewidth=1.5)[0]
            # Draw error bars.
            ax.fill_between(throughput.index,
                            throughput - std,
                            throughput + std,
                            color=line.get_color(),
                            alpha=0.3)

    ax.set_title(title)
    ax.set_xlabel(x_column)
    ax.set_ylabel(y_columns)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.grid()

def main(args) -> None:
    df = pd.read_csv(args.results)

    # Replace -1's with 0's.
    def replace_with_zero(df, label):
        df[label] = df[label].apply(lambda x: x if x > 0 else 0)
    replace_with_zero(df, 'write_output.start_throughput_1s.p90')
    replace_with_zero(df, 'read_output.start_throughput_1s.p90')
    replace_with_zero(df, 'write_output.latency.median_ms')
    replace_with_zero(df, 'read_output.latency.median_ms')

    # Abbreviate fields.
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    df['num_acceptors'] = (df['num_acceptor_groups'] *
                           df['num_acceptors_per_group'])
    df['batch_size'] = df['workload.num_operations']
    df['write_throughput'] = (df['write_output.start_throughput_1s.p90'] *
                              df['batch_size'])
    df['write_latency'] = df['write_output.latency.median_ms']
    df['read_throughput'] = (df['read_output.start_throughput_1s.p90'] *
                             df['batch_size'])
    df['read_latency'] = df['read_output.latency.median_ms']
    df['throughput'] = df['read_throughput'] + df['write_throughput']
    df['latency'] = df['read_latency'] + df['write_latency']

    num_figures = 58
    fig, ax = plt.subplots(num_figures, 1,
                           figsize=(6.4, num_figures * 4.8 * 1.25))
    axes = iter(ax)

    for num_clients in [1000, 3000, 5000]:
        for batch_size in [10, 50, 100]:
            for y_columns in ['throughput', 'latency']:
                plot(
                    df=df[
                        (df['workload_label'] == 'read_only_v1') &
                        (df['num_clients'] == num_clients) &
                        (df['batch_size'] == batch_size)
                    ],
                    ax=next(axes),
                    grouping_columns=('num_proxy_leaders', 'num_acceptors'),
                    x_column='num_replicas',
                    y_columns=[y_columns],
                    title=(f'100% reads, {num_clients} clients, ' +
                           f'batch size = {batch_size}'),
                )
    for batch_size in [5, 10, 50, 100]:
        for y_columns in ['throughput', 'latency']:
            plot(
                df=df[
                    (df['workload_label'] == 'read_only_v2') &
                    (df['batch_size'] == batch_size)
                ],
                ax=next(axes),
                grouping_columns=('num_clients',),
                x_column='num_replicas',
                y_columns=[y_columns],
                title=(f'100% reads, batch size = {batch_size}'),
            )
    for batch_size in [5, 10, 50, 100]:
        for y_columns in ['throughput', 'latency']:
            plot(
                df=df[
                    (df['workload_label'] == 'write_only_v1') &
                    (df['batch_size'] == batch_size) &
                    (df['num_proxy_leaders'] == df['num_replicas'])
                ],
                ax=next(axes),
                grouping_columns=('num_clients',),
                x_column='num_replicas',
                y_columns=[y_columns],
                title=(f'100% writes, num proxy leaders == num replicas, ' +
                       f'batch size = {batch_size}'),
            )
    for batch_size in [5, 10, 50, 100]:
        for y_columns in ['throughput', 'latency']:
            plot(
                df=df[
                    (df['workload_label'] == 'write_only_v1') &
                    (df['batch_size'] == batch_size) &
                    (df['num_proxy_leaders'] == 2 * df['num_replicas'])
                ],
                ax=next(axes),
                grouping_columns=('num_clients',),
                x_column='num_replicas',
                y_columns=[y_columns],
                title=(f'100% writes, num proxy leaders == 2 * num replicas, ' +
                       f'batch size = {batch_size}'),
            )
    for y_columns in ['throughput', 'latency']:
        plot(
            df=df[
                (df['workload_label'] == 'write_only_v2') &
                (df['num_proxy_leaders'] == 2) &
                (df['num_acceptor_groups'] == df['num_replicas'])
            ],
            ax=next(axes),
            grouping_columns=('num_clients',),
            x_column='num_replicas',
            y_columns=[y_columns],
            title=(f'100% writes, num proxy leaders == 2, batch size = 50, ' +
                   f'num_acceptor_groups == num_replicas'),
        )
    for y_columns in ['throughput', 'latency']:
        plot(
            df=df[
                (df['workload_label'] == 'write_only_v2') &
                (df['num_proxy_leaders'] == 6) &
                (df['num_acceptor_groups'] == df['num_replicas'])
            ],
            ax=next(axes),
            grouping_columns=('num_clients',),
            x_column='num_replicas',
            y_columns=[y_columns],
            title=(f'100% writes, num proxy leaders == 6, batch size = 50, '),
        )
    for y_columns in ['throughput', 'latency']:
        plot(
            df=df[
                (df['workload_label'] == 'write_only_v2') &
                (df['num_proxy_leaders'] == 2) &
                (df['num_acceptor_groups'] == df['num_replicas'] + 1)
            ],
            ax=next(axes),
            grouping_columns=('num_clients',),
            x_column='num_replicas',
            y_columns=[y_columns],
            title=(f'100% writes, num proxy leaders == 6, batch size = 50, ' +
                   f'num_acceptor_groups == num_replicas + 1'),
        )
    for y_columns in ['throughput', 'latency']:
        plot(
            df=df[
                (df['workload_label'] == 'write_only_v3') &
                (df['num_acceptor_groups'] == df['num_replicas'])
            ],
            ax=next(axes),
            grouping_columns=('num_clients',),
            x_column='num_replicas',
            y_columns=[y_columns],
            title=(f'100% writes, num proxy leaders == 6, batch size = 50' +
                   f'num_acceptor_groups == num_replicas'),
        )
    for y_columns in ['throughput', 'latency']:
        plot(
            df=df[
                (df['workload_label'] == 'write_only_v3') &
                (df['num_acceptor_groups'] == df['num_replicas'] + 1)
            ],
            ax=next(axes),
            grouping_columns=('num_clients',),
            x_column='num_replicas',
            y_columns=[y_columns],
            title=(f'100% writes, num proxy leaders == 6, batch size = 50, ' +
                   f'num_acceptor_groups == num_replicas + 1'),
        )
    for y_columns in ['throughput', 'latency']:
        plot(
            df=df[(df['workload_label'] == 'read_only_v3')],
            ax=next(axes),
            grouping_columns=('num_clients',),
            x_column='num_replicas',
            y_columns=[y_columns],
            title=(f'100% reads, batch size = 50'),
        )
    for read_fraction in [0.9, 0.6]:
        for y_columns in ['throughput', 'latency']:
            plot(
                df=df[
                    (df['workload_label'] == 'mixed_v1') &
                    (df['workload.read_fraction'] == read_fraction)
                ],
                ax=next(axes),
                grouping_columns=('num_clients',),
                x_column='num_replicas',
                y_columns=[y_columns],
                title=(f'{read_fraction} reads, batch size = 50'),
            )


    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        help='results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='batched_read_scale_hyperparameter.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
