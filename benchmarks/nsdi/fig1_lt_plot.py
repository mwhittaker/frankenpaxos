# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 14}
matplotlib.rc('font', **font)

from .. import parser_util
from typing import Any, List
import argparse
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
    throughput = grouped['start_throughput_1s.p90'].agg(np.mean).sort_index()
    latency = grouped['latency.median_ms'].agg(np.mean).sort_index()
    print(throughput)
    print(latency)
    print()
    ax.plot(throughput, latency, '.-', label=label, linewidth=2)


def make_figure(output_filename: str, multipaxos_df: pd.DataFrame,
                epaxos_df: pd.DataFrame, bpaxos_df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    print('Multipaxos')
    plot_latency_throughput(multipaxos_df, ax, 'MultiPaxos')
    for (conflict_rate, group) in epaxos_df.groupby('workload.conflict_rate'):
        print(f'EPaxos {conflict_rate}')
        plot_latency_throughput(group, ax, f'EPaxos ({conflict_rate})')
    for (conflict_rate, group) in bpaxos_df.groupby('workload.conflict_rate'):
        print(f'BPaxos {conflict_rate}')
        plot_latency_throughput(group, ax, f'BPaxos ({conflict_rate})')
    ax.set_title('')
    ax.set_xlabel('Throughput (commands per second)')
    ax.set_ylabel('Median latency (ms)')
    ax.legend(loc='best')
    ax.grid()
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def main(args) -> None:
    multipaxos_df = add_num_clients(pd.read_csv(args.multipaxos_results))
    epaxos_df = add_num_clients(pd.read_csv(args.epaxos_results))
    bpaxos_df = add_num_clients(pd.read_csv(args.bpaxos_results))

    make_figure(
        args.outputf1,
        multipaxos_df[multipaxos_df['f'] == 1],
        epaxos_df[epaxos_df['f'] == 1],
        bpaxos_df[bpaxos_df['f'] == 1],
    )
    make_figure(
        args.outputf2,
        multipaxos_df[multipaxos_df['f'] == 2],
        # EPaxos at conflict rate 0.1 is super noisy, so we remove it.
        epaxos_df[(epaxos_df['f'] == 2) &
                  (epaxos_df['workload.conflict_rate'] != 0.1)],
        bpaxos_df[bpaxos_df['f'] == 2],
    )


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--multipaxos_results',
                        type=argparse.FileType('r'),
                        help='Multipaxos results.csv file')
    parser.add_argument('--epaxos_results',
                        type=argparse.FileType('r'),
                        help='EPaxos results.csv file')
    parser.add_argument('--bpaxos_results',
                        type=argparse.FileType('r'),
                        help='BPaxos results.csv file')
    parser.add_argument('--outputf1',
                        type=str,
                        default='nsdi_fig1_lt_f1.pdf',
                        help='Output filename')
    parser.add_argument('--outputf2',
                        type=str,
                        default='nsdi_fig1_lt_f2.pdf',
                        help='Output filename')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
