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


MARKERS = itertools.cycle(['o', '*', '^', 's', 'P'])


def add_num_clients(df: pd.DataFrame) -> pd.DataFrame:
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    return df


def plot_lt(df: pd.DataFrame, ax: plt.Axes) -> None:
    grouped = df.groupby('server_options.flush_every_n')
    for flush_every_n, n_group in grouped:
        grouped = n_group.groupby('num_clients')
        throughput = grouped['throughput'].agg(np.mean).sort_index()
        throughput_std = grouped['throughput'].agg(np.std).sort_index().fillna(0)
        latency = grouped['latency'].agg(np.mean).sort_index()
        latency_std = grouped['latency'].agg(np.std).sort_index().fillna(0)
        print(throughput_std)
        ax.errorbar(throughput / 100000, latency, yerr=latency_std,
                    xerr=throughput_std / 100000, fmt='.-', label=flush_every_n,
                    barsabove=True)



def main(args) -> None:
    df = add_num_clients(pd.read_csv(args.results))

    # Abbreviate fields.
    df['throughput'] = df['start_throughput_1s.p90']
    df['latency'] = df['latency.median_ms']

    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    plot_lt(df, ax)
    ax.set_title('')
    ax.set_xlabel('Throughput (100,000 commands per second)')
    ax.set_ylabel('Latency (ms)')
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
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
                        default='unreplicated_lt_hyperparameter.pdf',
                        help='Output filename.')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
