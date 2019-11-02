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


def avg_latency(df):
    return df['latency.median_ms'].agg(np.mean)


def std_latency(df):
    return df['latency.median_ms'].agg(np.std)


def avg_tput(df):
    return df['stop_throughput_1s.p90'].agg(np.mean)


def std_tput(df):
    return df['stop_throughput_1s.p90'].agg(np.std)


def add_num_clients(df: pd.DataFrame) -> pd.DataFrame:
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    return df


def barchart(output_filename: str,
             labels: List[str],
             data: List[float],
             yerr: List[float],
             color: List[str]) -> None:

    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    x_pos = range(len(data))
    ax.bar(x_pos, data, yerr=yerr, align='center', capsize=10, color=color)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, rotation=-45, ha='left')
    ax.set_title('')
    ax.set_xlabel('')
    ax.set_ylabel('Throughput (commands per second)')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def main(args) -> None:
    unbatched_super_df = \
        add_num_clients(pd.read_csv(args.unbatched_coupled_multipaxos_results))
    unbatched_df = \
        add_num_clients(pd.read_csv(args.unbatched_multipaxos_results))
    batched_super_df = \
        add_num_clients(pd.read_csv(args.batched_coupled_multipaxos_results))
    batched_df = \
        add_num_clients(pd.read_csv(args.batched_multipaxos_results))

    # We only look at data with 10 * 100 clients batched and 20 * 200 batched.
    unbatched_super_df = \
        unbatched_super_df[unbatched_super_df['num_clients'] == 1000]
    unbatched_df = \
        unbatched_df[unbatched_df['num_clients'] == 1000]
    batched_super_df = \
        batched_super_df[batched_super_df['num_clients'] == 4000]
    batched_df = \
        batched_df[batched_df['num_clients'] == 4000]

    barchart(
        output_filename = args.output_unbatched_throughput,
        labels = [
            'coupled',
            'decoupled',
            '3 proxy leaders',
            '4 proxy leaders',
            '5 proxy leaders',
            '6 proxy leaders',
            '7 proxy leaders',
            '8 proxy leaders',
            '2 acceptor groups',
        ],
        data = [
            avg_tput(unbatched_super_df),
            avg_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 2]),
            avg_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 3]),
            avg_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 4]),
            avg_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 5]),
            avg_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 6]),
            avg_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 7]),
            avg_tput(unbatched_df[(unbatched_df['num_proxy_leaders'] == 8) &
                                  (unbatched_df['num_acceptor_groups'] == 1)]),
            avg_tput(unbatched_df[(unbatched_df['num_proxy_leaders'] == 8) &
                                  (unbatched_df['num_acceptor_groups'] == 2)]),
        ],
        yerr = [
            std_tput(unbatched_super_df),
            std_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 2]),
            std_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 3]),
            std_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 4]),
            std_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 5]),
            std_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 6]),
            std_tput(unbatched_df[unbatched_df['num_proxy_leaders'] == 7]),
            std_tput(unbatched_df[(unbatched_df['num_proxy_leaders'] == 8) &
                                  (unbatched_df['num_acceptor_groups'] == 1)]),
            std_tput(unbatched_df[(unbatched_df['num_proxy_leaders'] == 8) &
                                  (unbatched_df['num_acceptor_groups'] == 2)]),
        ],
        color = [
            'C0',
            'C1',
            'C2',
            'C2',
            'C2',
            'C2',
            'C2',
            'C2',
            'C3',
        ],
    )

    barchart(
        output_filename = args.output_batched_throughput,
        labels = [
            'coupled',
            'decoupled',
            '3 proxy replicas',
            '4 proxy replicas',
            '5 proxy replicas',
            '6 proxy replicas',
        ],
        data = [
            avg_tput(batched_super_df),
            avg_tput(batched_df[batched_df['num_proxy_replicas'] == 2]),
            avg_tput(batched_df[batched_df['num_proxy_replicas'] == 3]),
            avg_tput(batched_df[batched_df['num_proxy_replicas'] == 4]),
            avg_tput(batched_df[batched_df['num_proxy_replicas'] == 5]),
            avg_tput(batched_df[batched_df['num_proxy_replicas'] == 6]),
        ],
        yerr = [
            std_tput(batched_super_df),
            std_tput(batched_df[batched_df['num_proxy_replicas'] == 2]),
            std_tput(batched_df[batched_df['num_proxy_replicas'] == 3]),
            std_tput(batched_df[batched_df['num_proxy_replicas'] == 4]),
            std_tput(batched_df[batched_df['num_proxy_replicas'] == 5]),
            std_tput(batched_df[batched_df['num_proxy_replicas'] == 6]),
        ],
        color = [
            'C0',
            'C1',
            'C2',
            'C2',
            'C2',
            'C2',
        ],
    )


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unbatched_coupled_multipaxos_results',
                        type=argparse.FileType('r'),
                        help='Unbatched Super MultiPaxos results.csv file')
    parser.add_argument('--unbatched_multipaxos_results',
                        type=argparse.FileType('r'),
                        help='Unbatched MultiPaxos results.csv file')
    parser.add_argument('--batched_coupled_multipaxos_results',
                        type=argparse.FileType('r'),
                        help='Batched Super MultiPaxos results.csv file')
    parser.add_argument('--batched_multipaxos_results',
                        type=argparse.FileType('r'),
                        help='Batched MultiPaxos results.csv file')
    parser.add_argument(
        '--output_unbatched_throughput',
        type=str,
        default='eurosys_fig4_multipaxos_ablation_unbatched_throughput.pdf',
        help='Unbatched output filename')
    parser.add_argument(
        '--output_batched_throughput',
        type=str,
        default='eurosys_fig4_multipaxos_ablation_batched_throughput.pdf',
        help='Bathced output filename')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
