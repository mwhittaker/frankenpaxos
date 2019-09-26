# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 14}
matplotlib.rc('font', **font)

from .. import parser_util
from typing import Any, List
import argparse
import itertools
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re

def avg_latency(df):
    return df['latency.median_ms'].agg(np.mean)

def std_latency(df):
    return df['latency.median_ms'].agg(np.std)

def avg_throughput(df):
    return df['stop_throughput_1s.p90'].agg(np.mean)

def std_throughput(df):
    return df['stop_throughput_1s.p90'].agg(np.std)

def add_num_clients(df: pd.DataFrame) -> pd.DataFrame:
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    return df

def latency_figure(output_filename: str,
                   superbpaxos_df: pd.DataFrame,
                   simplebpaxos_df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    labels = (
        'coupled',
        '3 leaders',
        '4 leaders',
        '5 leaders',
        '6 leaders',
        '7 leaders'
    )
    latencies = [
        avg_latency(superbpaxos_df),
        avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 3]),
        avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 4]),
        avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 5]),
        avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 6]),
        avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 7]),
    ]
    yerr = [
        std_latency(superbpaxos_df),
        std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 3]),
        std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 4]),
        std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 5]),
        std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 6]),
        std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 7]),
    ]
    x_pos = range(len(latencies))
    ax.bar(x_pos, latencies, yerr=yerr, align='center', capsize=10)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, rotation=-45)
    ax.set_title('')
    ax.set_xlabel('')
    ax.set_ylabel('Median latency (ms)')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def throughput_figure(output_filename: str,
                   superbpaxos_df: pd.DataFrame,
                   simplebpaxos_df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    labels = (
        'coupled',
        '3 leaders',
        '4 leaders',
        '5 leaders',
        '6 leaders',
        '7 leaders'
    )
    latencies = [
        avg_throughput(superbpaxos_df),
        avg_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 3]),
        avg_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 4]),
        avg_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 5]),
        avg_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 6]),
        avg_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 7]),
    ]
    yerr = [
        std_throughput(superbpaxos_df),
        std_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 3]),
        std_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 4]),
        std_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 5]),
        std_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 6]),
        std_throughput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 7]),
    ]
    x_pos = range(len(latencies))
    ax.bar(x_pos, latencies, yerr=yerr, align='center', capsize=10)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, rotation=-45)
    ax.set_title('')
    ax.set_xlabel('')
    ax.set_ylabel('Throughput (commands per second)')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def main(args) -> None:
    superbpaxos_df = add_num_clients(pd.read_csv(args.superbpaxos_results))
    simplebpaxos_df = add_num_clients(pd.read_csv(args.simplebpaxos_results))

    latency_figure(
        args.output_low_load_latency,
        superbpaxos_df[superbpaxos_df['num_clients'] == 1],
        simplebpaxos_df[simplebpaxos_df['num_clients'] == 1],
    )
    latency_figure(
        args.output_high_load_latency,
        superbpaxos_df[superbpaxos_df['num_clients'] == 600],
        simplebpaxos_df[simplebpaxos_df['num_clients'] == 600],
    )
    throughput_figure(
        args.output_high_load_throughput,
        superbpaxos_df[superbpaxos_df['num_clients'] == 600],
        simplebpaxos_df[simplebpaxos_df['num_clients'] == 600],
    )


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--superbpaxos_results',
        type=argparse.FileType('r'),
        help='Super BPaxos results.csv file'
    )
    parser.add_argument(
        '--simplebpaxos_results',
        type=argparse.FileType('r'),
        help='Simple BPaxos results.csv file'
    )
    parser.add_argument(
        '--output_low_load_latency',
        type=str,
        default='nsdi_fig2_ablation_low_load_latency.pdf',
        help='Output filename'
    )
    parser.add_argument(
        '--output_high_load_latency',
        type=str,
        default='nsdi_fig2_ablation_high_load_latency.pdf',
        help='Output filename'
    )
    parser.add_argument(
        '--output_high_load_throughput',
        type=str,
        default='nsdi_fig2_ablation_high_load_throughput.pdf',
        help='Output filename'
    )
    return parser

if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
