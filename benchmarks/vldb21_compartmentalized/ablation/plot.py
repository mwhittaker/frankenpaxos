# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 16}
matplotlib.rc('font', **font)

from typing import Any, List
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re


def avg_tput(df):
    return df['throughput'].agg(np.mean)


def std_tput(df):
    return df['throughput'].agg(np.std)


def add_num_clients(df: pd.DataFrame) -> pd.DataFrame:
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    return df


def barchart(output_filename: str, labels: List[str], data: List[float],
             yerr: List[float], color: List[str]) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.0))
    x_pos = range(len(data))
    ax.bar(x_pos, data, yerr=yerr, align='center', capsize=10, color=color)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, rotation=-45, ha='left')
    ax.set_title('')
    ax.set_xlabel('')
    ax.set_ylabel('Throughput (thousands)')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def main(args) -> None:
    coupled_df = add_num_clients(pd.read_csv(args.coupled_results))
    coupled_df = coupled_df[coupled_df['num_clients'] == 1000]
    coupled_df['throughput'] = coupled_df['start_throughput_1s.p90']
    coupled_df['latency'] = coupled_df['latency.median_ms']

    comp_df = add_num_clients(pd.read_csv(args.compartmentalized_results))
    comp_df['throughput'] = comp_df['write_output.start_throughput_1s.p90']
    comp_df['latency'] = comp_df['write_output.latency.median_ms']

    npl = comp_df['num_proxy_leaders']
    na = comp_df['num_acceptor_groups'] * comp_df['num_acceptors_per_group']
    nr = comp_df['num_replicas']

    dfs = [
        coupled_df,
        comp_df[(npl == 2) & (na == 3) & (nr == 2)],
        comp_df[(npl == 3) & (na == 3) & (nr == 2)],
        comp_df[(npl == 4) & (na == 3) & (nr == 2)],
        comp_df[(npl == 5) & (na == 3) & (nr == 2)],
        comp_df[(npl == 6) & (na == 3) & (nr == 2)],
        comp_df[(npl == 7) & (na == 3) & (nr == 2)],
        comp_df[(npl == 7) & (na == 3) & (nr == 3)],
        comp_df[(npl == 8) & (na == 3) & (nr == 3)],
        comp_df[(npl == 9) & (na == 3) & (nr == 3)],
        comp_df[(npl == 10) & (na == 3) & (nr == 3)],
    ]
    barchart(
        output_filename=args.output,
        labels=[
            'coupled',
            'decoupled',
            '3 proxy leaders',
            '4 proxy leaders',
            '5 proxy leaders',
            '6 proxy leaders',
            '7 proxy leaders',
            '3 replicas',
            '8 proxy leaders',
            '9 proxy leaders',
            '10 proxy leaders',
        ],
        data=[avg_tput(df) / 1000 for df in dfs],
        yerr=[std_tput(df) / 1000 for df in dfs],
        color=['C0', 'C1', 'C2', 'C2', 'C2', 'C2', 'C2', 'C3', 'C2', 'C2', 'C2'],
    )


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--coupled_results',
                        type=argparse.FileType('r'),
                        help='Super MultiPaxos results.csv file')
    parser.add_argument('--compartmentalized_results',
                        type=argparse.FileType('r'),
                        help='Compartmentalized MultiPaxos results.csv file')
    parser.add_argument(
        '--output',
        type=str,
        default='ablation.pdf',
        help='Output filename')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
