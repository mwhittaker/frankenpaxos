# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 16}
matplotlib.rc('font', **font)

from typing import Any, List
import argparse
import itertools
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re


MARKERS = itertools.cycle(['o', '*', '^', 's', 'P', 'x', '1'])


def outlier_throughput(df: pd.DataFrame) -> float:
    cutoff = 0.5 * df['throughput'].max()
    return df[df['throughput'] >= cutoff]['throughput'].mean()


def outlier_throughput_std(df: pd.DataFrame) -> float:
    cutoff = 0.5 * df['throughput'].max()
    return df[df['throughput'] >= cutoff]['throughput'].std()


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
    df['read_throughput'] = df['read_output.start_throughput_1s.p90']
    df['read_latency'] = df['read_output.latency.median_ms']
    df['write_throughput'] = df['write_output.start_throughput_1s.p90']
    df['write_latency'] = df['write_output.latency.median_ms']
    df['throughput'] = df['write_throughput'] + df['read_throughput']
    df['latency'] = df['write_latency'] + df['read_latency']

    # Select data.
    # df = df[
    #     # 100% reads.
    #     (df['workload_label'] == 'all_reads_v1') |
    #
    #     # 90% reads.
    #     (
    #         (df['workload_label'] == 'some_writes_v1') &
    #         (df['workload.read_fraction'] == 0.9) &
    #         (df['num_acceptors'] == 3) &
    #         (df['num_clients'] == 2500)
    #     ) |
    #
    #     # 60% reads.
    #     (
    #         (df['workload_label'] == 'some_writes_v1') &
    #         (df['workload.read_fraction'] == 0.6) &
    #         (df['num_acceptors'] == 3) &
    #         (df['num_clients'] == 500)
    #     ) |
    #
    #     # 0% reads.
    #     (
    #         (df['workload_label'] == 'read_scale_v1') &
    #         (df['workload.read_fraction'] == 0.0)
    #     )
    # ]

    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.0))

    by_read_fraction = df.groupby('workload.read_fraction')
    for (read_fraction, group) in by_read_fraction:
        by_replicas = group.groupby('num_replicas')
        throughput = by_replicas.apply(outlier_throughput).sort_index() / 1000
        std = by_replicas.apply(outlier_throughput_std).sort_index() / 1000
        lines = ax.plot(
            throughput.index,
            throughput,
            '-',
            marker = next(MARKERS),
            label=f'{int(read_fraction * 100)}% reads',
            linewidth=1.5
        )
        ax.fill_between(throughput.index,
                        throughput - std,
                        throughput + std,
                        color=lines[0].get_color(),
                        alpha=0.3)

    ax.set_ylim(ymin=0)
    ax.set_title('')
    ax.set_xlabel('Number of replicas')
    ax.set_ylabel('Throughput\n(thousands cmds/second)')
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1), ncol=2)
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
                        default='weak_read_scale.pdf',
                        help='Output filename')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
