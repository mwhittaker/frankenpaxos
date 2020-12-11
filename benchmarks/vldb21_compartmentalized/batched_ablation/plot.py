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
    coupled_df = coupled_df[coupled_df['num_clients'] == 4000]
    coupled_df['throughput'] = coupled_df['start_throughput_1s.p90']
    coupled_df['latency'] = coupled_df['latency.median_ms']

    comp_df = add_num_clients(pd.read_csv(args.compartmentalized_results))
    comp_df['throughput'] = comp_df['write_output.start_throughput_1s.p90']
    comp_df['latency'] = comp_df['write_output.latency.median_ms']

    batch = comp_df['batcher_options.batch_size']
    npr = comp_df['num_proxy_replicas']

    dfs = [
        coupled_df,
        comp_df[(batch == 10) & (npr == 2)],
        comp_df[(batch == 50) & (npr == 2)],
        comp_df[(batch == 100) & (npr == 2)],
        comp_df[(batch == 100) & (npr == 3)],
        comp_df[(batch == 100) & (npr == 4)],
        comp_df[(batch == 100) & (npr == 5)],
    ]
    barchart(
        output_filename=args.output,
        labels=[
            'coupled',
            'decoupled',
            'batch size 50',
            'batch size 100',
            '3 unbatchers',
            '4 unbatchers',
            '5 unbatchers',
        ],
        data=[avg_tput(df) / 1000 for df in dfs],
        yerr=[std_tput(df) / 1000 for df in dfs],
        color=['C0', 'C1', 'C2', 'C2', 'C3', 'C3', 'C3'],
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
