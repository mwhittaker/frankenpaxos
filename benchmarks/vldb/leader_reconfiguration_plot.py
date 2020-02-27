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


def plot_throughput(ax: plt.Axes, n: int, s: pd.Series, sample_every: int) -> None:
    tput = pd_util.throughput(s, 1000, trim=True)[::sample_every]
    ax.plot_date(tput.index, tput, fmt='-', label=f'{n} clients')


def plot_latency(ax: plt.Axes, n: int, s: pd.Series, sample_every: int) -> None:
    median = s.rolling('1000ms').median()
    p95 = s.rolling('1000ms').quantile(0.95)
    label = '1 client' if n == 1 else f'{n} clients'
    line = ax.plot_date(s.index[::sample_every],
                        median[::sample_every],
                        label=label,
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
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8 * 0.7),
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
    reconfigurations = [
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=1, microsecond=379000),
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=2, microsecond=381000),
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=3, microsecond=383000),
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=4, microsecond=385000),
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=5, microsecond=387000),
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=6, microsecond=388000),
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=7, microsecond=390000),
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=8, microsecond=392000),
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=9, microsecond=394000),
        datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=10, microsecond=396000),
    ]
    failure = datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=15, microsecond=373000)
    recover = datetime.datetime(
            2020, 2, 22, hour=22, minute=11, second=20, microsecond=374000)

    for axes in ax:
        for t in reconfigurations:
            axes.axvline(x=origin + (t - naive_start_time), color='black')
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



def plot_violin(n1: Tuple[np.array, np.array],
                n4: Tuple[np.array, np.array],
                n8: Tuple[np.array, np.array],
                ylabel: str,
                output_filename: str) -> None:
    num_plots = 3
    fig, ax = plt.subplots(1, num_plots, figsize=(6.4 * 1.25, 4.8))

    for (axes, data, color, n) in zip(ax, [n1,n4,n8], ['C0','C1','C2'], [1,4,8]):
        # Plot data.
        parts = axes.violinplot(data, widths=0.5, showextrema=False)

        # Set color.
        for pc in parts['bodies']:
            pc.set_facecolor(color)
            pc.set_edgecolor('black')
            pc.set_alpha(1)

        # Set y-axis label.
        if n == 1:
            axes.set_ylabel(ylabel)

        # Set x-axis labels.
        axes.set_xticklabels(['', '0s-10s', '10s-20s'])

        # Set title.
        axes.set_title('1 client' if n == 1 else f'{n} clients')

        # Show median, quartiles, and min/max.
        p25 = [np.percentile(xs, 25) for xs in data]
        medians = [np.percentile(xs, 50) for xs in data]
        p75 = [np.percentile(xs, 75) for xs in data]
        mins = [np.min(xs) for xs in data]
        maxs = [np.max(xs) for xs in data]

        axes.scatter([1, 2], medians, marker='o', color='white', s=30, zorder=3)
        axes.vlines([1, 2], p25, p75, color='k', linestyle='-', lw=7)
        axes.vlines([1, 2], mins, maxs, color='k', linestyle='-', lw=1)

    fig.set_tight_layout(True)
    fig.savefig(output_filename)
    print(f'Wrote plot to {output_filename}.')


def violin(n1: pd.DataFrame,
           n4: pd.DataFrame,
           n8: pd.DataFrame,
           throughput_filename: str,
           latency_filename: str) -> None:
    ten = datetime.datetime(1970, 1, 1, second=10)
    twenty = datetime.datetime(1970, 1, 1, second=20)

    # https://stackoverflow.com/a/11686764/3187068
    def reject_outliers(data, p=99):
        return data[data < np.percentile(data, p)]

    def throughput(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        before = df[:ten]
        during = df[ten:twenty]
        return (
            pd_util.throughput(before['delta'], 1000, trim=True).to_numpy(),
            pd_util.throughput(during['delta'], 1000, trim=True).to_numpy(),
        )

    def latency(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        before = df[:ten]
        during = df[ten:twenty]
        return (
            reject_outliers((before['latency_nanos'] / 1e6).to_numpy()),
            reject_outliers((during['latency_nanos'] / 1e6).to_numpy()),
        )

    plot_violin(
        n1 = throughput(n1),
        n4 = throughput(n4),
        n8 = throughput(n8),
        ylabel = 'Throughput (commands/second)',
        output_filename = throughput_filename)

    plot_violin(
        n1 = latency(n1),
        n4 = latency(n4),
        n8 = latency(n8),
        ylabel = 'Latency (ms)',
        output_filename = latency_filename)


def main(args) -> None:
    # Read the data.
    (f1n1, start_time) = read_data(args.f1n1, args.drop_head, args.drop_tail)
    (f1n4, _) = read_data(args.f1n4, args.drop_head, args.drop_tail + 5)
    (f1n8, _) = read_data(args.f1n8, args.drop_head, args.drop_tail)
    (f2n1, _) = read_data(args.f2n1, args.drop_head, args.drop_tail)
    (f2n4, _) = read_data(args.f2n4, args.drop_head, args.drop_tail + 1)
    (f2n8, _) = read_data(args.f2n8, args.drop_head, args.drop_tail + 1)

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
    violin(
        n1=f1n1,
        n4=f1n4,
        n8=f1n8,
        throughput_filename=args.output_violin_throughput,
        latency_filename=args.output_violin_latency,
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
                        default='vldb_leader_reconfiguration_f=1.pdf',
                        help='f=1 output filename')
    parser.add_argument('--output_f2',
                        type=str,
                        default='vldb_leader_reconfiguration_f=2.pdf',
                        help='f=2 output filename')
    parser.add_argument(
        '--output_violin_throughput',
        type=str,
        default='vldb_leader_reconfiguration_violin_throughput.pdf',
        help='f=1 violin throughput output filename')
    parser.add_argument(
        '--output_violin_latency',
        type=str,
        default='vldb_leader_reconfiguration_violin_latency.pdf',
        help='f=1 violin latency output filename')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
