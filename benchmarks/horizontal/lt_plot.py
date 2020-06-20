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


def plot_latency_throughput(df: pd.DataFrame, ax: plt.Axes, latency_name: str,
                            label: str) -> None:
    grouped = df.groupby('num_clients')
    throughput = grouped['throughput'].mean().sort_index()
    latency = grouped[latency_name].mean().sort_index()
    ax.plot(throughput, latency, '.-', label=label, linewidth=2)
    # for (t, l) in zip(throughput, latency):
    #     ax.annotate('{:.3}'.format(l), xy=(t, l))


def main(args) -> None:
    df = add_num_clients(pd.read_csv(args.results))

    # Shorten column names.
    df['throughput'] = df['start_throughput_1s.p90']
    df['median_latency'] = df['latency.median_ms']
    df['p99_latency'] = df['latency.p99_ms']
    df['alpha'] = df['leader_options.alpha']

    # Plot with.
    num_plots = 2
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))
    for alpha in df['alpha'].unique():
        plot_latency_throughput(df[df['alpha'] == alpha], ax[0],
                                'median_latency', f'alpha = {alpha}')
        plot_latency_throughput(df[df['alpha'] == alpha], ax[1],
                                'p99_latency', f'alpha = {alpha}')

    ax[0].set_ylabel('Median latency (ms)')
    ax[1].set_ylabel('P99 latency (ms)')
    for a in ax:
        a.set_title('')
        a.set_xlabel('Throughput')
        a.legend(loc='best')
        a.grid()
    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        help='results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='horizontal_lt.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
