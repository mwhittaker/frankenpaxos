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


# def latency_figure(output_filename: str, superbpaxos_df: pd.DataFrame,
#                    simplebpaxos_df: pd.DataFrame) -> None:
#     fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
#     labels = ('coupled', '3 leaders', '4 leaders', '5 leaders', '6 leaders',
#               '7 leaders')
#     latencies = [
#         avg_latency(superbpaxos_df),
#         avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 3]),
#         avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 4]),
#         avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 5]),
#         avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 6]),
#         avg_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 7]),
#     ]
#     yerr = [
#         std_latency(superbpaxos_df),
#         std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 3]),
#         std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 4]),
#         std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 5]),
#         std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 6]),
#         std_latency(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 7]),
#     ]
#     x_pos = range(len(latencies))
#     ax.bar(x_pos, latencies, yerr=yerr, align='center', capsize=10)
#     ax.set_xticks(x_pos)
#     ax.set_xticklabels(labels, rotation=-45)
#     ax.set_title('')
#     ax.set_xlabel('')
#     ax.set_ylabel('Median latency (ms)')
#     ax.legend(loc='best')
#     fig.savefig(output_filename, bbox_inches='tight')
#     print(f'Wrote plot to {output_filename}.')


def barchart(output_filename: str,
             labels: List[str],
             data: List[float],
             yerr: List[float]) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
    x_pos = range(len(data))
    ax.bar(x_pos, data, yerr=yerr, align='center', capsize=10)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, rotation=-45, ha='left')
    ax.set_title('')
    ax.set_xlabel('')
    ax.set_ylabel('Throughput (commands per second)')
    # ax.legend(loc='best')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')

# def throughput_figure(output_filename: str, superbpaxos_df: pd.DataFrame,
#                       simplebpaxos_df: pd.DataFrame) -> None:
#     fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.8))
#     labels = ('coupled', '3 leaders', '4 leaders', '5 leaders', '6 leaders',
#               '7 leaders')
#     latencies = [
#         avg_tput(superbpaxos_df),
#         avg_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 3]),
#         avg_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 4]),
#         avg_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 5]),
#         avg_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 6]),
#         avg_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 7]),
#     ]
#     yerr = [
#         std_tput(superbpaxos_df),
#         std_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 3]),
#         std_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 4]),
#         std_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 5]),
#         std_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 6]),
#         std_tput(simplebpaxos_df[simplebpaxos_df['num_leaders'] == 7]),
#     ]
#     x_pos = range(len(latencies))
#     ax.bar(x_pos, latencies, yerr=yerr, align='center', capsize=10)
#     ax.set_xticks(x_pos)
#     ax.set_xticklabels(labels, rotation=-45)
#     ax.set_title('')
#     ax.set_xlabel('')
#     ax.set_ylabel('Throughput (commands per second)')
#     ax.legend(loc='best')
#     fig.savefig(output_filename, bbox_inches='tight')
#     print(f'Wrote plot to {output_filename}.')


def main(args) -> None:
    unbatched_super_df = \
        add_num_clients(pd.read_csv(args.unbatched_coupled_multipaxos_results))
    unbatched_df = \
        add_num_clients(pd.read_csv(args.unbatched_multipaxos_results))
    # batched_super_df = \
    #     add_num_clients(pd.read_csv(args.batched_coupled_multipaxos_results))
    # batched_df = \
    #     add_num_clients(pd.read_csv(args.batched_multipaxos_results))

    # We only look at data with 10 * 100 clients.
    unbatched_super_df = \
        unbatched_super_df[unbatched_super_df['num_clients'] == 1000]
    unbatched_df = \
        unbatched_df[unbatched_df['num_clients'] == 1000]
    # batched_super_df = \
    #     batched_super_df[batched_super_df['num_clients'] == 1000]
    # batched_df = \
    #     batched_df[batched_df['num_clients'] == 1000]

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
        ]
    )

    # latency_figure(
    #     args.output_low_load_latency,
    #     superbpaxos_df[superbpaxos_df['num_clients'] == 1],
    #     simplebpaxos_df[simplebpaxos_df['num_clients'] == 1],
    # )
    # latency_figure(
    #     args.output_high_load_latency,
    #     superbpaxos_df[superbpaxos_df['num_clients'] == 600],
    #     simplebpaxos_df[simplebpaxos_df['num_clients'] == 600],
    # )
    # throughput_figure(
    #     args.output_high_load_throughput,
    #     superbpaxos_df[superbpaxos_df['num_clients'] == 600],
    #     simplebpaxos_df[simplebpaxos_df['num_clients'] == 600],
    # )


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
