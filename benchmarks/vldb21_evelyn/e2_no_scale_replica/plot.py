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


ALPHA = 1
VERBOSE = False


def vprint(*args) -> None:
    if VERBOSE:
        print(*args)


def add_num_clients(df: pd.DataFrame) -> pd.DataFrame:
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    return df


def plot_throughput(df: pd.DataFrame, ax: plt.Axes,
                    fw: float, label: str) -> None:
    def outlier_throughput(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].mean() / 100000

    def outlier_throughput_std(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].std() / 100000

    # Draw throughput.
    grouped = df.groupby('num_replicas')
    vprint(f'# {label}')
    for (name, group) in grouped:
        vprint(f'## {name}')
        vprint(group[['throughput', 'latency']])
    throughput = grouped.apply(outlier_throughput).sort_index()
    std = grouped.apply(outlier_throughput_std).sort_index()
    vprint(f'throughput = {throughput}')
    vprint(f'std = {std}')
    vprint()
    line = ax.plot(throughput.index, throughput,
                   '.-', label=label, linewidth=2)[0]

    # Draw ideal throughput.
    ns = pd.Series(range(2, 7))
    ideal = ns * ALPHA / ((ns * fw) + ((1 - fw) * 0.66))
    ax.plot(ns,
            ideal,
            '--',
            linewidth=2,
            color=line.get_color(),
            label='_nolegend_')

    # Draw error bars.
    ax.plot(throughput.index,
            throughput - std,
            '-',
            linewidth=0.5,
            color=line.get_color(),
            alpha=0.5)
    ax.plot(throughput.index,
            throughput + std,
            '-',
            linewidth=0.5,
            color=line.get_color(),
            alpha=0.5)
    ax.fill_between(throughput.index,
                    throughput - std,
                    throughput + std,
                    color=line.get_color(),
                    alpha=0.4)


def main(args) -> None:
    global VERBOSE
    VERBOSE = args.verbose

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

    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    plot_throughput(df[df['workload_label'] == 0.0], ax, 1.0, '100% writes')
    plot_throughput(df[df['workload_label'] == 0.4], ax, 0.6, '60% writes')
    plot_throughput(df[df['workload_label'] == 0.8], ax, 0.2, '20% writes')
    plot_throughput(df[df['workload_label'] == 0.95], ax, 0.05, '5% writes')
    plot_throughput(df[df['workload_label'] == 1.0], ax, 0.0, '0% writes')

    ax.set_title('')
    ax.set_xlabel('Number of replicas')
    ax.set_ylabel('Throughput\n(100,000 commands per second)')
    ax.legend(loc='best')
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
                        default='e2_no_scale_replica.pdf',
                        help='Output filename.')
    parser.add_argument('--verbose', action='store_true')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
