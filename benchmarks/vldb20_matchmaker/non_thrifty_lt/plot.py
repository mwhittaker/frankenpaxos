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


def plot_latency_throughput(df: pd.DataFrame, ax: plt.Axes,
                            label: str, style: str) -> None:
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
    line = ax.plot(throughput, latency, style, label=label, linewidth=2)[0]
    ax.fill_betweenx(latency,
                     throughput - throughput_std,
                     throughput + throughput_std,
                     color = line.get_color(),
                     alpha=0.25)


def main(args) -> None:
    thrifty_df = add_num_clients(pd.read_csv(args.thrifty_results))
    non_thrifty_df = add_num_clients(pd.read_csv(args.non_thrifty_results))

    # Abbreviate values.
    for df in [thrifty_df, non_thrifty_df]:
        df['throughput'] = df['start_throughput_1s.p90']
        df['latency'] = df['latency.median_ms']

    # Filter high clients.
    thrifty_df = thrifty_df[thrifty_df['num_clients'] <= 200]
    non_thrifty_df = non_thrifty_df[non_thrifty_df['num_clients'] <= 200]

    # Plot.
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    plot_latency_throughput(thrifty_df, ax, 'thrifty', '.-')
    plot_latency_throughput(non_thrifty_df, ax, 'non-thrifty', '.--')

    ax.set_title('')
    ax.set_xlabel('Throughput (commands per second)')
    ax.set_ylabel('Median latency (ms)')
    ax.grid()
    ax.legend()
    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--thrifty_results',
                        type=argparse.FileType('r'),
                        help='Thrifty results.csv file')
    parser.add_argument('--non_thrifty_results',
                        type=argparse.FileType('r'),
                        help='Non-thrifty results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='non_thrifty_lt.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
