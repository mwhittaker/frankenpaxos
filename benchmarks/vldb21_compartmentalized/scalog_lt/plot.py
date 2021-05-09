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


def plot_lt(df: pd.DataFrame, ax: plt.Axes, marker: str, label: str) -> None:
    grouped = df.groupby('num_clients')
    throughput = grouped['throughput'].agg(np.mean).sort_index() / 1000
    throughput_std = grouped['throughput'].agg(np.std).sort_index() / 1000
    latency = grouped['latency'].agg(np.mean).sort_index()
    line = ax.plot(throughput, latency, marker, label=label, linewidth=2)[0]
    ax.fill_betweenx(latency,
                     throughput - throughput_std,
                     throughput + throughput_std,
                     color = line.get_color(),
                     alpha=0.25)


def main(args) -> None:
    scalog_df = pd.read_csv(args.scalog_results)
    compartmentalized_df = pd.read_csv(args.compartmentalized_results)

    # Abbreviate fields.
    for df in [compartmentalized_df]:
        df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
        df['throughput'] = df['write_output.start_throughput_1s.p90']
        df['latency'] = df['write_output.latency.median_ms']
    for df in [scalog_df]:
        df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
        df['throughput'] = df['output.start_throughput_1s.p90']
        df['latency'] = df['output.latency.median_ms']

    # Prune the data.
    noyolo_scalog_df = scalog_df[
        (scalog_df['num_proxy_replicas'] == 0) &
        (scalog_df['num_clients'] >= 500) &
        (scalog_df['num_clients'] <= 2800)
    ]
    yolo_scalog_df = scalog_df[
        (scalog_df['num_proxy_replicas'] > 0) &
        (scalog_df['num_clients'] >= 700)
    ]
    compartmentalized_df = compartmentalized_df[
        (compartmentalized_df['num_clients'] >= 500)
    ]


    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.0))
    plot_lt(noyolo_scalog_df, ax, '^-', 'Scalog')
    plot_lt(yolo_scalog_df, ax, 'o-', 'Partially Compartmentalized Scalog')
    plot_lt(compartmentalized_df, ax, 's-', 'Compartmentalized Multipaxos')
    ax.set_title('')
    ax.set_xlabel('Throughput (thousands of commands per second)')
    ax.set_ylabel('Median latency (ms)')
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1))
    ax.grid()
    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--scalog_results',
                        type=argparse.FileType('r'),
                        help='Scalog results.csv file')
    parser.add_argument('--compartmentalized_results',
                        type=argparse.FileType('r'),
                        help='Compartmentalized Multipaxos results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='scalog_lt.pdf',
                        help='Output filename')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
