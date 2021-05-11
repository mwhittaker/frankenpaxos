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
    compartmentalized_df = pd.read_csv(args.compartmentalized_results)
    unreplicated_df = pd.read_csv(args.unreplicated_results)

    # Abbreviate fields.
    for df in [compartmentalized_df]:
        df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
        df['size'] = df['workload.write_size_mean']
        df['throughput'] = df['write_output.start_throughput_1s.p90']
        df['latency'] = df['write_output.latency.median_ms']
    for df in [unreplicated_df]:
        df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
        df['size'] = df['workload.size_mean']
        df['throughput'] = df['start_throughput_1s.p90']
        df['latency'] = df['latency.median_ms']

    # Plot the data.
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.0))
    plot_lt(
        compartmentalized_df[compartmentalized_df['size'] == 100],
        ax,
        'o-',
        'Compartmentalized MultiPaxos (100 bytes)',
    )
    plot_lt(
        unreplicated_df[unreplicated_df['size'] == 100],
        ax,
        's-',
        'Unreplicated (100 bytes)',
    )
    plot_lt(
        compartmentalized_df[
            (compartmentalized_df['size'] == 1000) &
            (compartmentalized_df['num_clients'] <= 600)
        ],
        ax,
        '*-',
        'Compartmentalized MultiPaxos (1000 bytes)',
    )
    plot_lt(
        unreplicated_df[
            (unreplicated_df['size'] == 1000) &
            (unreplicated_df['num_clients'] <= 800)
        ],
        ax,
        '^-',
        'Unreplicated (1000 bytes)',
    )

    ax.set_title('')
    ax.set_xlabel('Throughput (thousands of commands per second)')
    ax.set_ylabel('Median latency (ms)')
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1))
    ax.grid()
    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--compartmentalized_results',
                        type=argparse.FileType('r'),
                        help='Compartmentalized Multipaxos results.csv file')
    parser.add_argument('--unreplicated_results',
                        type=argparse.FileType('r'),
                        help='Unreplicated results.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='bigger_values_compartmentalized_lt.pdf',
                        help='Output filename')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
