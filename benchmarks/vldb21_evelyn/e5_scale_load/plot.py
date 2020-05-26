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
MARKERS = itertools.cycle(['o', '*', '^', 's', 'P'])
VERBOSE = False


def vprint(*args) -> None:
    if VERBOSE:
        print(*args)


def add_num_clients(df: pd.DataFrame) -> pd.DataFrame:
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    return df


def rf(num_writers: int, num_clients: int) -> int:
    return int((1 - (num_writers / num_clients)) * 100)


def plot_throughput(df: pd.DataFrame, ax: plt.Axes, label: str) -> None:
    def outlier_throughput(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].mean() / 100000

    def outlier_throughput_std(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].std() / 100000

    # Draw throughput.
    grouped = df.groupby('workload_label')
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
                   '-', marker = next(MARKERS), label=label, linewidth=1.5)[0]

    # Draw error bars.
    ax.fill_between(throughput.index,
                    throughput - std,
                    throughput + std,
                    color=line.get_color(),
                    alpha=0.3)


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
    df['workload_label'] /= 100 * 1000

    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    plot_throughput(df[df['num_replicas'] == 2], ax, '2 replicas')
    plot_throughput(df[df['num_replicas'] == 3], ax, '3 replicas')
    plot_throughput(df[df['num_replicas'] == 4], ax, '4 replicas')
    plot_throughput(df[df['num_replicas'] == 5], ax, '5 replicas')
    plot_throughput(df[df['num_replicas'] == 6], ax, '6 replicas')

    ax.invert_xaxis()
    ax.set_title('')
    ax.set_xlabel('Write load (100,000 writes per second)')
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
                        default='e5_scale_load.pdf',
                        help='Output filename.')
    parser.add_argument('--verbose', action='store_true')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
