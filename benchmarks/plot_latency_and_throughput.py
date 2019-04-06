from . import util
import numpy as np
import argparse
import matplotlib.pyplot as plt
import os
import pandas as pd

def plot_latency(args: argparse.Namespace,
                 ax: plt.Axes,
                 df: pd.DataFrame) -> None:
    latency_ms = df['latency_nanos'] / 1e6
    if args.stds:
        mu = latency_ms.mean()
        sigma = latency_ms.std()
        df = df[np.abs(latency_ms - mu) <= (args.stds * sigma)]

    ax.plot_date(latency_ms.index,
                 latency_ms.rolling('250ms').mean(),
                 label='250ms',
                 fmt='-',
                 alpha=0.5)
    ax.plot_date(latency_ms.index,
                 latency_ms.rolling('500ms').mean(),
                 label='500ms',
                 fmt='-',
                 alpha=0.7)
    ax.plot_date(latency_ms.index,
                 latency_ms.rolling('1s').mean(),
                 label='1s',
                 fmt='-')
    ax.set_title('Latency')
    ax.set_xlabel('Time')
    ax.set_ylabel('Latency (ms)')

def plot_throughput(args: argparse.Namespace,
                    ax: plt.Axes,
                    df: pd.DataFrame) -> None:
    # Plot throughput.
    ax.plot_date(df.index,
                 util.throughput(df, 250),
                 label='250ms',
                 fmt='-',
                 alpha=0.5)
    ax.plot_date(df.index,
                 util.throughput(df, 500),
                 label='500ms',
                 fmt='-',
                 alpha=0.7)
    ax.plot_date(df.index,
                 util.throughput(df, 1000),
                 label='1s',
                 fmt='-')
    ax.set_title('Throughput')
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
    fig, ax = plt.subplots(2, 1, figsize=(6.4, 2 * 4.8))
    plot_latency(args, ax[0], df)
    plot_throughput(args, ax[1], df)
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
        '-s', '--stds',
        type=float,
        default=None,
        help='Latency values that deviate by more that <stds> stds are stripped'
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
