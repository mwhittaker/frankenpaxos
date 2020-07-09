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
    reconfigurations = [
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=29, microsecond=680000),
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=30, microsecond=682000),
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=31, microsecond=684000),
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=32, microsecond=686000),
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=33, microsecond=688000),
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=34, microsecond=690000),
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=35, microsecond=691000),
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=36, microsecond=693000),
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=37, microsecond=695000),
        datetime.datetime(
            2020, 6, 23, hour=4, minute=52, second=38, microsecond=697000),
    ]
    failure = datetime.datetime(
        2020, 6, 23, hour=4, minute=52, second=43, microsecond=675000)
    recover = datetime.datetime(
        2020, 6, 23, hour=4, minute=52, second=48, microsecond=675000)

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
    ax[1].set_ylabel('Throughput\n(cmds/second)')
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
    fig, ax = plt.subplots(1, num_plots, figsize=(6.4 * 1.25, 4.8 * 0.75))

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

    # Plot the data.
    plot(
        n1=f1n1,
        n4=f1n4,
        n8=f1n8,
        output_filename=args.output,
        f=1,
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
    parser.add_argument('--output',
                        type=str,
                        default='horizontal_leader_reconfiguration.pdf',
                        help='Output filename')
    parser.add_argument(
        '--output_violin_throughput',
        type=str,
        default='horizontal_leader_reconfiguration_violin_throughput.pdf',
        help='Violin throughput output filename')
    parser.add_argument(
        '--output_violin_latency',
        type=str,
        default='horizontal_leader_reconfiguration_violin_latency.pdf',
        help='Violin latency output filename')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
