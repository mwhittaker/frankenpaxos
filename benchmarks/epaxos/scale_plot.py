# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')

from .. import parser_util
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import textwrap


def wrapped(s: str, width: int = 60) -> str:
    return '\n'.join(textwrap.wrap(s, width))


def plot(df: pd.DataFrame, ax, column: str, pretty_column: str) -> None:
    for (f, group) in df.groupby(['f']):
        stats = group.groupby(['num_clients'])[column].agg([np.mean, np.std])
        mean = stats['mean']
        std = stats['std'].fillna(0)

        line = ax.plot(mean, '.-', label=f'f={f}')[0]
        color = line.get_color()
        ax.fill_between(stats.index, mean - std, mean + std,
                        color=color, alpha=0.25)

    ax.set_title(wrapped(f'{pretty_column} vs number of clients', 100))
    ax.set_xlabel('Number of clients')
    ax.set_ylabel(pretty_column)
    ax.legend()
    ax.grid()


def main(args) -> None:
    df = pd.read_csv(args.results)
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']

    num_plots = 2
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))
    plot(df, ax[0], 'latency.median_ms', 'Median latency (ms)')
    plot(df, ax[1], 'throughput_1s.p90', 'P90 throughput (1 second windows)')

    fig.set_tight_layout(True)
    fig.savefig(args.output)
    print(f'Wrote plot to {args.output}.')


if __name__ == '__main__':
    parser = parser_util.get_plot_parser('epaxos_scale.pdf')
    main(parser.parse_args())
