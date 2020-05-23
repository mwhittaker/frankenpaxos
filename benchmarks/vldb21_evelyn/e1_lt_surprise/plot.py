# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 14}
matplotlib.rc('font', **font)

from typing import Any, List
import argparse
import datetime
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re


def add_num_clients(df: pd.DataFrame) -> pd.DataFrame:
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    return df


def plot_latency_throughput(df: pd.DataFrame, ax: plt.Axes, label: str) -> None:
    def outlier_throughput(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].mean() / 100000

    def outlier_latency(g: pd.DataFrame) -> float:
        cutoff = 5 * g['latency'].min()
        return g[g['latency'] <= cutoff]['latency'].mean()

    grouped = df.groupby('num_clients')
    for (name, group) in grouped:
        print(f'# {name}')
        print(group[['throughput', 'latency']])
    throughput = grouped.apply(outlier_throughput).sort_index()
    latency = grouped.apply(outlier_latency).sort_index()
    print(f'throughput = {throughput}.')
    print(f'latency = {latency}.')
    print()
    ax.plot(throughput, latency, '.-', label=label, linewidth=2)


def main(args) -> None:
    df = add_num_clients(pd.read_csv(args.results))

    # Replace -1's with 0's.
    def replace_with_zero(df, label):
        df[label] = df[label].apply(lambda x: x if x > 0 else 0)
    replace_with_zero(df, 'write_output.start_throughput_1s.p90')
    replace_with_zero(df, 'read_output.start_throughput_1s.p90')
    replace_with_zero(df, 'write_output.latency.median_ms')
    replace_with_zero(df, 'read_output.latency.median_ms')

    # Sum read and write values.
    df['throughput'] = (df['write_output.start_throughput_1s.p90'] +
                        df['read_output.start_throughput_1s.p90'])
    df['latency'] = (df['write_output.latency.median_ms'] +
                     df['read_output.latency.median_ms'])

    # Split data based on read fraction.
    flush_period = pd.to_timedelta(df['leader_options.noop_flush_period'])
    all_writes_df = df[flush_period == datetime.timedelta(seconds=0)]
    all_reads_df = df[flush_period == datetime.timedelta(microseconds=500)]
    half_reads_df = df[flush_period == datetime.timedelta(milliseconds=1)]

    # Plot with.
    for (b, output) in [(True, args.output_with), (False, args.output_without)]:
        fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
        plot_latency_throughput(all_writes_df, ax, '100% writes')
        plot_latency_throughput(all_reads_df, ax, '100% reads')
        if b:
            plot_latency_throughput(half_reads_df, ax, '50% reads')

        ax.set_title('')
        ax.set_xlabel('Throughput (100,000 commands per second)')
        ax.set_ylabel('Median latency (ms)')
        ax.legend(loc='best')
        ax.grid()
        fig.savefig(output, bbox_inches='tight')
        print(f'Wrote plot to {output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        help='results.csv file')
    parser.add_argument('--output_with',
                        type=str,
                        default='e1_lt_surprise_with.pdf',
                        help='Output filename with 50% reads.')
    parser.add_argument('--output_without',
                        type=str,
                        default='e1_lt_surprise_without.pdf',
                        help='Output filename without 50% reads.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
