# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 14}
matplotlib.rc('font', **font)

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

def main(args) -> None:
    df = add_num_clients(pd.read_csv(args.results))

    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))

    ax.plot(
            # df['write_output.start_throughput_1s.p90'] +
            df['read_output.start_throughput_1s.p90'],
            # df['write_output.latency.median_ms'] +
            df['read_output.latency.median_ms'], 'o')

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df[['num_clients', 'read_output.start_throughput_1s.p90', 'read_output.latency.median_ms']])

    # plot_latency_throughput(multipaxos_df, ax, 'MultiPaxos')
    # for (conflict_rate, group) in epaxos_df.groupby('workload.conflict_rate'):
    #     print(f'EPaxos {conflict_rate}')
    #     plot_latency_throughput(group, ax, f'EPaxos ({conflict_rate})')
    # for (conflict_rate, group) in bpaxos_df.groupby('workload.conflict_rate'):
    #     print(f'BPaxos {conflict_rate}')
    #     plot_latency_throughput(group, ax, f'BPaxos ({conflict_rate})')
    ax.set_title('')
    ax.set_xlabel('Throughput (commands per second)')
    ax.set_ylabel('Median latency (ms)')
    # ax.legend(loc='best')
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
                        default='e1_lt_surprise.pdf',
                        help='Output filename')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
