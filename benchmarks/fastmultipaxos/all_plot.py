from textwrap import wrap
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd


def wrapped(s: str, width: int = 60) -> str:
    return '\n'.join(wrap(s, width))


def plot(df: pd.DataFrame, ax, column: str, pretty_column: str) -> None:
    def translate_paxos_variant(name: str) -> str:
        if name == 'CLASSIC_ROUND_ROBIN':
            return 'MultiPaxos'
        elif name == 'MIXED_ROUND_ROBIN':
            return 'Fast MultiPaxos'
        else:
            assert False, name

    def translate_name(name):
        return (translate_paxos_variant(name[0]), name[1])

    grouped = df.groupby([
        'round_system_type',
        'client_repropose_period_seconds'
    ])

    for (name, group) in grouped:
        stats = group.groupby('num_clients').agg([np.mean, np.std])
        mean = stats[column]['mean']
        std = stats[column]['std'].fillna(0)
        line = ax.plot(mean, '.-', label=translate_name(name))[0]
        color = line.get_color()
        ax.fill_between(stats.index, mean - std, mean + std, color=color,
                        alpha=0.25)

    ax.set_title(wrapped(
        f'{pretty_column} for values of client_repropose_period_seconds'
    ))
    ax.set_xlabel('Number of clients')
    ax.set_ylabel(pretty_column)
    ax.grid()
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))


def main(args) -> None:
    df = pd.read_csv(args.results)

    # See [1] for figure size defaults. We add an extra plot at the bottom for
    # a textual note.
    #
    # [1]: https://matplotlib.org/api/_as_gen/matplotlib.pyplot.figure.html
    num_plots = 4
    fig, ax = plt.subplots(num_plots, 1, figsize=(1.5 * 6.4, num_plots * 4.8))

    plot(df, ax[0], 'median_latency_ms', 'Median latency')
    plot(df, ax[1], 'p90_latency_ms', 'P90 latency')
    plot(df, ax[2], 'median_1_second_throughput',
                    'Median throughput (1 second windows)')
    plot(df, ax[3], 'p90_1_second_throughput',
                    'P90 throughput (1 second windows)')

    fig.set_tight_layout(True)
    fig.savefig(args.output)
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'results',
        type=str,
        help='results.csv file'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='.',
        help='Output filename'
    )
    return parser


if __name__ == '__main__':
    main(get_parser().parse_args())
