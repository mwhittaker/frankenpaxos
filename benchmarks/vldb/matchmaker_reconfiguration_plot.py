# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 14}
matplotlib.rc('font', **font)

from .. import pd_util
from typing import Any, List, Tuple
import argparse
import datetime
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re


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


def plot_throughput(ax: plt.Axes, n: int, s: pd.Series, sample_every: int) -> None:
    tput = pd_util.throughput(s, 1000, trim=True)[::sample_every]
    ax.plot_date(tput.index, tput, fmt='-', label=f'{n} clients')


def plot_latency(ax: plt.Axes, n: int, s: pd.Series, sample_every: int) -> None:
    median = s.rolling('1000ms').median()
    p95 = s.rolling('1000ms').quantile(0.95)
    line = ax.plot_date(s.index[::sample_every],
                        median[::sample_every],
                        label=f'{n} clients',
                        fmt='-')[0]
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
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8),
                           sharex=True)

    # Plot data.
    plot_latency(ax[0], 1, n1['latency_nanos'] / 1e6, sample_every)
    plot_latency(ax[0], 4, n4['latency_nanos'] / 1e6, sample_every)
    plot_latency(ax[0], 8, n8['latency_nanos'] / 1e6, sample_every)
    plot_throughput(ax[1], 1, n1['delta'], sample_every)
    plot_throughput(ax[1], 4, n4['delta'], sample_every)
    plot_throughput(ax[1], 8, n8['delta'], sample_every)

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
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=37, microsecond=863000),
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=38, microsecond=876000),
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=39, microsecond=878000),
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=40, microsecond=880000),
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=41, microsecond=881000),
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=42, microsecond=883000),
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=43, microsecond=885000),
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=44, microsecond=887000),
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=45, microsecond=889000),
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=46, microsecond=891000),
    ]
    failure = \
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=51, microsecond=857000)
    recover = \
        datetime.datetime(2020, 2, 22, hour=17, minute=52, second=56, microsecond=857000)
    reconfigure = \
        datetime.datetime(2020, 2, 22, hour=17, minute=53, second=1, microsecond=857000)

    for axes in ax:
        for t in matchmaker_reconfigurations:
            axes.axvline(x=origin + (t - naive_start_time), color='black')
        axes.axvline(x=origin + (failure - naive_start_time), color='red',
                     ls='--')
        axes.axvline(x=origin + (recover - naive_start_time), color='black')
        axes.axvline(x=origin + (reconfigure - naive_start_time), color='blue',
                     ls='-.')

    # Write legend.
    handles, labels = ax[0].get_legend_handles_labels()
    legend = fig.legend(handles, labels,
                        bbox_to_anchor=(0.5, 1), loc='center', ncol=3)

    # Save figures.
    ax[1].set_xlabel('Time')
    ax[0].set_ylabel('Latency (ms)')
    ax[1].set_ylabel('Throughput (commands/second)')
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
                        default='vldb_matchmaker_reconfiguration_f=1.pdf',
                        help='f=1 output filename')
    parser.add_argument('--output_f2',
                        type=str,
                        default='vldb_matchmaker_reconfiguration_f=2.pdf',
                        help='f=2 output filename')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
