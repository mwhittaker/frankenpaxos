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
    grouped = df.groupby('num_clients')
    for (name, group) in grouped:
        print(f'# {name}')
        print(group[['throughput', 'latency']])
    throughput = grouped['throughput'].mean().sort_index()
    latency = grouped['latency'].mean().sort_index()
    print(f'throughput = {throughput}.')
    print(f'latency = {latency}.')
    print()
    ax.plot(throughput, latency, '.-', label=label, linewidth=2)


def main(args) -> None:
    df = add_num_clients(pd.read_csv(args.results))

    # Shorten column names.
    df['throughput'] = df['output.start_throughput_1s.p90']
    df['latency'] = df['output.latency.median_ms']
    ack = df['server_options.ack_noops_with_commands']
    f1 = df['server_options.use_f1_optimization']

    # Plot with.
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    plot_latency_throughput(df[(ack == False) & (f1 == False)], ax, 'no opts')
    plot_latency_throughput(df[f1 == True], ax, 'f = 1 opt')
    plot_latency_throughput(df[ack == True], ax, 'ack noops opt')

    ax.set_title('')
    ax.set_xlabel('Throughput')
    ax.set_ylabel('Median latency (ms)')
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
                        default='fasterpaxos_lt.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
