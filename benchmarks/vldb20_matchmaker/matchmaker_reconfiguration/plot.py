# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 14}
matplotlib.rc('font', **font)

from ... import pd_util
from typing import Any, List, Tuple
import argparse
import datetime
import itertools
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re


MARKERS = itertools.cycle(['v', 's', 'P'])


def read_data(file,
              drop_head: float,
              drop_tail: float) -> Tuple[pd.DataFrame, Any]:
    # Read the data.
    df = pd.read_csv(file, parse_dates=['start', 'stop'])

    # Chop off the head and tail.
    start_time = df['start'].iloc[0]
    end_time = df['start'].iloc[-1]
    new_start_time = start_time + pd.DateOffset(seconds=drop_head)
    new_end_time = end_time - pd.DateOffset(seconds=drop_tail)
    df = df[df['start'] >= new_start_time]
    df = df[df['start'] <= new_end_time]

    # Normalize the times so all data fits on same axes.
    df['delta'] = pd.Timestamp(0) + (df['start'] - new_start_time)
    df.index = df['delta']

    return (df, new_start_time)


def report_stats(df: pd.DataFrame, f: int, n: int) -> None:
    ten = datetime.datetime(1970, 1, 1, second=10)
    twenty = datetime.datetime(1970, 1, 1, second=20)
    before = df[:ten]
    during = df[ten:twenty]

    def throughput(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return (
            pd_util.throughput(before['delta'], 1000, trim=True).to_numpy(),
            pd_util.throughput(during['delta'], 1000, trim=True).to_numpy(),
        )

    def latency(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return (
            (before['latency_nanos'] / 1e6).to_numpy(),
            (during['latency_nanos'] / 1e6).to_numpy(),
        )

    def report(name: str, xs: np.array) -> None:
        print(name)
        print(f'- 25% = {np.percentile(xs, 25)}')
        print(f'- 50% = {np.percentile(xs, 50)}')
        print(f'- 75% = {np.percentile(xs, 75)}')
        print(f'- IQR = {np.percentile(xs, 75) - np.percentile(xs, 50)}')
        print(f'- stddev = {np.std(xs)}')

    tput_before, tput_during = throughput(df)
    report(f'[f={f}, n={n}] throughput before', tput_before)
    report(f'[f={f}, n={n}] throughput during', tput_during)

    latency_before, latency_during = latency(df)
    report(f'[f={f}, n={n}] latency before', latency_before)
    report(f'[f={f}, n={n}] latency during', latency_during)


def plot_throughput(ax: plt.Axes, n: int, s: pd.Series, sample_every: int,
                    marker: str) -> None:
    tput = pd_util.throughput(s, 1000, trim=True)[::sample_every]
    ax.plot_date(tput.index, tput, fmt='-', marker=marker, markevery=0.1,
                 label=f'{n} clients')


def plot_latency(ax: plt.Axes, n: int, s: pd.Series, sample_every: int,
                 marker: str) -> None:
    median = s.rolling('1000ms').median()
    p95 = s.rolling('1000ms').quantile(0.95)
    label = '1 client' if n == 1 else f'{n} clients'
    line = ax.plot_date(s.index[::sample_every],
                        median[::sample_every],
                        label=label,
                        fmt='-',
                        marker=marker,
                        markevery=0.1)[0]
    ax.fill_between(s.index[::sample_every], median[::sample_every],
                    p95[::sample_every], color=line.get_color(), alpha=0.25)


def plot(n1: pd.DataFrame,
         n4: pd.DataFrame,
         n8: pd.DataFrame,
         output_filename: str,
         f: int,
         start_time,
         sample_every: int):
    # Create figure.
    num_plots = 2
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8 * 0.5),
                           sharex=True)

    # Plot data.
    plot_latency(ax[0], 1, n1['latency_nanos'] / 1e6, sample_every, next(MARKERS))
    plot_latency(ax[0], 4, n4['latency_nanos'] / 1e6, sample_every, next(MARKERS))
    plot_latency(ax[0], 8, n8['latency_nanos'] / 1e6, sample_every, next(MARKERS))
    plot_throughput(ax[1], 1, n1['delta'], sample_every, next(MARKERS))
    plot_throughput(ax[1], 4, n4['delta'], sample_every, next(MARKERS))
    plot_throughput(ax[1], 8, n8['delta'], sample_every, next(MARKERS))

    # Format x ticks nicely.
    for axes in ax:
        axes.grid()
        # axes.legend(loc='best')
        axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%-M:%S'))
        for label in axes.get_xticklabels():
            label.set_ha('left')
            label.set_rotation(-20)

    # Draw vertical reconfiguration lines. The times are hardcoded and taken
    # from the Driver's output files. I know that's super jank, but oh well.
    origin = datetime.datetime(1970, 1, 1, second=0)
    naive_start_time = start_time.to_pydatetime().replace(tzinfo=None)
    matchmaker_reconfigurations = [
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=21, microsecond=46000),
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=22, microsecond=58000),
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=23, microsecond=60000),
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=24, microsecond=62000),
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=25, microsecond=64000),
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=26, microsecond=65000),
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=27, microsecond=67000),
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=28, microsecond=69000),
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=29, microsecond=71000),
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=30, microsecond=73000),
    ]
    failure = \
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=35, microsecond=40000)
    recover = \
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=40, microsecond=41000)
    reconfigure = \
        datetime.datetime(2020, 7, 2, hour=19, minute=30, second=45, microsecond=41000)

    for axes in ax:
        for t in matchmaker_reconfigurations:
            axes.axvline(x=origin + (t - naive_start_time), color='blue',
                         ls='-.')
        axes.axvline(x=origin + (failure - naive_start_time), color='red',
                     ls='--')
        axes.axvline(x=origin + (recover - naive_start_time), color='blue',
                     ls='-.')
        axes.axvline(x=origin + (reconfigure - naive_start_time), color='black')

    # Write legend.
    handles, labels = ax[0].get_legend_handles_labels()
    legend = fig.legend(handles, labels,
                        bbox_to_anchor=(0.5, 1), loc='center', ncol=3)

    # Save figures.
    ax[1].set_xlabel('Time')
    ax[0].set_ylabel('Latency (ms)')
    ax[1].set_ylabel('Throughput\n(cmds/second)')
    fig.set_tight_layout(True)
    fig.savefig(output_filename, bbox_extra_artists=(legend,),
                bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def main(args) -> None:
    # Read the data.
    (f1n1, start_time) = read_data(args.f1n1, args.drop_head, args.drop_tail)
    (f1n4, _) = read_data(args.f1n4, args.drop_head, args.drop_tail)
    (f1n8, _) = read_data(args.f1n8, args.drop_head, args.drop_tail)
    (f2n1, _) = read_data(args.f2n1, args.drop_head, args.drop_tail)
    (f2n4, _) = read_data(args.f2n4, args.drop_head, args.drop_tail)
    (f2n8, _) = read_data(args.f2n8, args.drop_head, args.drop_tail)

    # Plot the data.
    plot(
        n1=f1n1,
        n4=f1n4,
        n8=f1n8,
        output_filename=args.output_f1,
        f=1,
        start_time=start_time,
        sample_every=args.sample_every,
    )
    plot(
        n1=f2n1,
        n4=f2n4,
        n8=f2n8,
        output_filename=args.output_f2,
        f=2,
        start_time=start_time,
        sample_every=args.sample_every,
    )

    # Report stats.
    report_stats(f1n1, f=1, n=1)
    report_stats(f1n4, f=1, n=4)
    report_stats(f1n8, f=1, n=8)
    report_stats(f2n1, f=2, n=1)
    report_stats(f2n4, f=2, n=4)
    report_stats(f2n8, f=2, n=8)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--drop_head',
        type=float,
        default=0,
        help='Drop this number of seconds from the head of the benchmark.')
    parser.add_argument(
        '--drop_tail',
        type=float,
        default=0,
        help='Drop this number of seconds from the tail of the benchmark.')
    parser.add_argument(
        '--sample_every',
        type=int,
        default=1,
        help='Sample every n.')

    parser.add_argument('--f1n1',
                        type=argparse.FileType('r'),
                        help='f=1, n=1 data.csv file')
    parser.add_argument('--f1n4',
                        type=argparse.FileType('r'),
                        help='f=1, n=4 data.csv file')
    parser.add_argument('--f1n8',
                        type=argparse.FileType('r'),
                        help='f=1, n=8 data.csv file')
    parser.add_argument('--f2n1',
                        type=argparse.FileType('r'),
                        help='f=2, n=1 data.csv file')
    parser.add_argument('--f2n4',
                        type=argparse.FileType('r'),
                        help='f=2, n=4 data.csv file')
    parser.add_argument('--f2n8',
                        type=argparse.FileType('r'),
                        help='f=2, n=8 data.csv file')

    parser.add_argument('--output_f1',
                        type=str,
                        default='matchmaker_reconfiguration_f1.pdf',
                        help='f=1 output filename')
    parser.add_argument('--output_f2',
                        type=str,
                        default='matchmaker_reconfiguration_f2.pdf',
                        help='f=2 output filename')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
