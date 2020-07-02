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


def plot_latency_throughput(df: pd.DataFrame, ax: plt.Axes) -> None:
    def outlier_throughput(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].mean()

    def outlier_throughput_std(g: pd.DataFrame) -> float:
        cutoff = 0.5 * g['throughput'].max()
        return g[g['throughput'] >= cutoff]['throughput'].std()

    def outlier_latency(g: pd.DataFrame) -> float:
        cutoff = 5 * g['latency'].min()
        return g[g['latency'] <= cutoff]['latency'].mean()

    grouped = df.groupby('num_clients')
    for (name, group) in grouped:
        print(f'# {name}')
        print(group[['throughput', 'latency']])
    throughput = grouped.apply(outlier_throughput).sort_index()
    latency = grouped.apply(outlier_latency).sort_index()
    throughput_std = grouped.apply(outlier_throughput_std).sort_index()
    print(f'throughput = {throughput}.')
    print(f'latency = {latency}.')
    print()
    line = ax.plot(throughput, latency, '.-', linewidth=2)[0]
    ax.fill_betweenx(latency,
                     throughput - throughput_std,
                     throughput + throughput_std,
                     color = line.get_color(),
                     alpha=0.25)


def main(args) -> None:
    df = add_num_clients(pd.read_csv(args.results))

    # Abbreviate values.
    df['throughput'] = df['start_throughput_1s.p90']
    df['latency'] = df['latency.median_ms']

    # Plot.
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    plot_latency_throughput(df, ax)

    ax.set_title('')
    ax.set_xlabel('Throughput (commands per second)')
    ax.set_ylabel('Median latency (ms)')
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
                        default='matchmaker_multipaxos_lt.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
