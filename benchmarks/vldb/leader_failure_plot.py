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
              drop_tail: float,
              nudge: datetime.timedelta) -> Tuple[pd.DataFrame, Any]:
    # Read the data.
    df = pd.read_csv(file, parse_dates=['start', 'stop'])

    # Chop off the head and tail.
    start_time = df['start'].iloc[0]
    end_time = df['start'].iloc[-1]
    new_start_time = start_time + pd.DateOffset(seconds=drop_head) + nudge
    new_end_time = end_time - pd.DateOffset(seconds=drop_tail)
    df = df[df['start'] >= new_start_time]
    df = df[df['start'] <= new_end_time]

    # Normalize the times so all data fits on same axes.
    df['delta'] = pd.Timestamp(0) + (df['start'] - new_start_time)
    df.index = df['delta']

    return (df, new_start_time)


def split(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    for i in range(len(df)):
        if i == 0:
            continue

        if df.iloc[i]['start'] - df.iloc[i - 1]['start'] > pd.Timedelta('1s'):
            return df.iloc[:i], df[i:]

    raise ValueError("No split found.")


def plot_throughput(ax: plt.Axes, n: int, before: pd.Series, after: pd.Series,
                    sample_every: int) -> None:
    tput_before = pd_util.throughput(before, 1000, trim=True)[::sample_every]
    line = ax.plot_date(tput_before.index, tput_before, fmt='-') [0]
    tput_after = pd_util.throughput(after, 1000, trim=False)[::sample_every]
    line = ax.plot_date(tput_after.index, tput_after, fmt='-',
                        color=line.get_color())


def plot_latency(ax: plt.Axes, n: int, before: pd.Series, after: pd.Series,
                 sample_every: int) -> None:
    median_before = before.rolling('1000ms').median()
    p95_before = before.rolling('1000ms').quantile(0.95)
    line = ax.plot_date(before.index[::sample_every],
                        median_before[::sample_every],
                        label='_nolegend_',
                        fmt='-')[0]
    ax.fill_between(before.index[::sample_every],
                    median_before[::sample_every],
                    p95_before[::sample_every],
                    color=line.get_color(), alpha=0.25)

    median_after = after.rolling('1000ms').median()
    p95_after = after.rolling('1000ms').quantile(0.95)
    label = '1 client' if n == 1 else f'{n} clients'
    ax.plot_date(after.index[::sample_every],
                 median_after[::sample_every],
                 label=label,
                 color = line.get_color(),
                 fmt='-')
    ax.fill_between(after.index[::sample_every],
                    median_after[::sample_every],
                    p95_after[::sample_every],
                    color=line.get_color(), alpha=0.25)


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

    # Split data.
    n1_before, n1_after = split(n1)
    print('n1 split')
    n4_before, n4_after = split(n4)
    print('n4 split')
    n8_before, n8_after = split(n8)
    print('n8 split')

    # Plot data.
    plot_latency(ax[0], 1, n1_before['latency_nanos'] / 1e6,
                 n1_after['latency_nanos'] / 1e6, sample_every)
    plot_latency(ax[0], 4, n4_before['latency_nanos'] / 1e6,
                 n4_after['latency_nanos'] / 1e6, sample_every)
    plot_latency(ax[0], 8, n8_before['latency_nanos'] / 1e6,
                 n8_after['latency_nanos'] / 1e6, sample_every)
    ax[0].set_ylim([0, 2.0])

    plot_throughput(ax[1], 1, n1_before['delta'], n1_after['delta'], sample_every)
    plot_throughput(ax[1], 4, n4_before['delta'], n4_after['delta'], sample_every)
    plot_throughput(ax[1], 8, n8_before['delta'], n8_after['delta'], sample_every)

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
    failure = \
        datetime.datetime(2020, 2, 25, hour=4, minute=9, second=19, microsecond=347000)
    recover = \
        datetime.datetime(2020, 2, 25, hour=4, minute=9, second=23, microsecond=500000)

    for axes in ax:
        axes.axvline(x=origin + (failure - naive_start_time), color='red',
                     ls='--')
        axes.axvline(x=origin + (recover - naive_start_time), color='black')

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
    # Read the data. To align the benchmarks, we manually nudge. Ideally, we
    # would do some ultra fancy code to synchronize all the stuff, but it's not
    # worth it.
    (f1n1, start_time) = read_data(args.f1n1, args.drop_head, args.drop_tail,
                                   nudge=datetime.timedelta(seconds=0))
    (f1n4, _) = read_data(args.f1n4, args.drop_head, args.drop_tail,
                          nudge=datetime.timedelta(milliseconds=350))
    (f1n8, _) = read_data(args.f1n8, args.drop_head, args.drop_tail,
                          nudge=datetime.timedelta(milliseconds=450))

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

    parser.add_argument('--output_f1',
                        type=str,
                        default='vldb_matchmaker_reconfiguration_f=1.pdf',
                        help='f=1 output filename')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
