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
    paxos_df = pd.read_csv(args.compartmentalized_results)
    craq_df = pd.read_csv(args.craq_results)

    for df in [paxos_df, craq_df]:
        # Replace -1's with 0's.
        def replace_with_zero(df, label):
            df[label] = df[label].apply(lambda x: x if x > 0 else 0)
        replace_with_zero(df, 'write_output.start_throughput_1s.p90')
        replace_with_zero(df, 'read_output.start_throughput_1s.p90')
        replace_with_zero(df, 'write_output.latency.median_ms')
        replace_with_zero(df, 'read_output.latency.median_ms')

        # Abbreviate fields.
        df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
        df['read_throughput'] = df['read_output.start_throughput_1s.p90']
        df['read_latency'] = df['read_output.latency.median_ms']
        df['write_throughput'] = df['write_output.start_throughput_1s.p90']
        df['write_latency'] = df['write_output.latency.median_ms']
        df['throughput'] = df['write_throughput'] + df['read_throughput']
        df['latency'] = df['write_latency'] + df['read_latency']

    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.0))

    for (df, label) in [(paxos_df, 'Compartmentalized MultiPaxos'),
                        (craq_df, 'CRAQ')]:
        grouped = df.groupby('workload.point_fraction')
        throughput = grouped.apply(outlier_throughput).sort_index() / 1000
        std = grouped.apply(outlier_throughput_std).sort_index() / 1000
        lines = ax.plot(
            throughput.index,
            throughput,
            '-',
            marker = next(MARKERS),
            label=label,
            linewidth=1.5
        )
        ax.fill_between(throughput.index,
                        throughput - std,
                        throughput + std,
                        color=lines[0].get_color(),
                        alpha=0.3)

    ax.set_ylim(ymin=0)
    ax.set_title('')
    ax.set_xlabel('Skew')
    ax.set_ylabel('Throughput (thousands)')
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1))
    ax.grid()
    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument('--compartmentalized_results',
                        type=argparse.FileType('r'),
                        help='compartmentalized_results.csv file')
    parser.add_argument('--craq_results',
                        type=argparse.FileType('r'),
                        help='craq_results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='compartmentalized_skew.pdf',
                        help='Output filename')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
