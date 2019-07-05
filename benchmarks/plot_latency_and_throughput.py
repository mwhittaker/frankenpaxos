# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')

from . import pd_util
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd


def plot_latency(ax: plt.Axes, latency_ms: pd.Series) -> None:
    ax.plot_date(latency_ms.index,
                 latency_ms.rolling('500ms').median(),
                 label='500ms',
                 fmt='-',
                 alpha=0.5)
    ax.plot_date(latency_ms.index,
                 latency_ms.rolling('1s').median(),
                 label='1s',
                 fmt='-')
    ax.set_title('Median latency')
    ax.set_xlabel('Time')
    ax.set_ylabel('Median latency (ms)')


def plot_throughput(ax: plt.Axes, start: pd.Series, stop: pd.Series) -> None:
    # Plot throughput.
    ax.plot_date(pd_util.throughput(start, 1000, trim=True).index,
                 pd_util.throughput(start, 1000, trim=True),
                 label='start',
                 fmt='-')
    ax.plot_date(pd_util.throughput(stop, 1000, trim=True).index,
                 pd_util.throughput(stop, 1000, trim=True),
                 label='stop',
                 fmt='-',
                 alpha=0.7)
    ax.set_title('Throughput (1 second windows)')
    ax.set_xlabel('Time')
    ax.set_ylabel('Throughput')


def main(args) -> None:
    df = pd.read_csv(args.data_csv, parse_dates=['start', 'stop'])
    df.index = df['start']

    # Drop first bit of data.
    start_time = df['start'].iloc[0]
    new_start_time = start_time + pd.DateOffset(seconds=args.drop)
    df = df[df['start'] >= new_start_time]

    # See [1] for figure size defaults.
    #
    # [1]: https://matplotlib.org/api/_as_gen/matplotlib.pyplot.figure.html
    num_plots = 2
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))
    plot_latency(ax[0], df['latency_nanos'] / 1e6)
    plot_throughput(ax[1], df['start'], df['stop'])
    for axes in ax:
        axes.grid()
        axes.legend(loc='best')
        for label in axes.get_xticklabels():
            label.set_ha('right')
            label.set_rotation(20)
    fig.set_tight_layout(True)
    fig.savefig(args.output)
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'data_csv',
        type=str,
        help='data.csv file'
    )
    parser.add_argument(
        '-d', '--drop',
        type=float,
        default=0,
        help='Drop this number of seconds from the beginning of the benchmark.'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='latency_and_throughput.pdf',
        help='Output filename'
    )
    return parser


if __name__ == '__main__':
    from pandas.plotting import register_matplotlib_converters
    register_matplotlib_converters()
    main(get_parser().parse_args())
