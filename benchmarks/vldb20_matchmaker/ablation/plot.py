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


def plot_throughput(ax: plt.Axes, label: str, color: str,
                    s: pd.Series, sample_every: int) -> None:
    tput = pd_util.throughput(s, 250, trim=True)[::sample_every]
    ax.plot_date(tput.index, tput,
                 fmt='-', color=color, markevery=0.4, label='_nolegend_')


def plot_latency(ax: plt.Axes, label: str, color: str,
                 s: pd.Series, sample_every: int) -> None:
    median = s.rolling('500ms').median()
    max_latency = s.rolling('500ms').max()

    # ax.set_yscale('log')
    line = ax.plot_date(s.index[::sample_every], median[::sample_every],
                        fmt='-', color=color, markevery=0.2, label=label)[0]
    # ax.plot_date(s.index[::sample_every], max_latency[::sample_every],
    #              fmt='--', color=color, label='_nolegend_')
    ax.fill_between(s.index[::sample_every], median[::sample_every],
                    max_latency[::sample_every], color=line.get_color())


def plot(baseline: pd.DataFrame,
         gc: pd.DataFrame,
         phase1: pd.DataFrame,
         matchmaking: pd.DataFrame,
         output_filename: str,
         start_time,
         sample_every: int):
    # Create figure.
    num_rows = 2
    num_columns = 4
    fig, ax = plt.subplots(num_rows, num_columns,
                           figsize=(num_columns * 6.4 * 0.5, num_rows * 4.8 * 0.5),
                           sharex=True, sharey='row')

    # Draw vertical reconfiguration lines. The times are hardcoded and taken
    # from the Driver's output files. I know that's super jank, but oh well. We
    # draw the verical lines before the plots, so that the vertical lines don't
    # hide the latency spikes.
    origin = datetime.datetime(1970, 1, 1, second=0)
    naive_start_time = start_time.to_pydatetime().replace(tzinfo=None)
    reconfigurations = [
        datetime.datetime(2020, 6, 25, hour=16, minute=33, second=56, microsecond=113000),
        datetime.datetime(2020, 6, 25, hour=16, minute=33, second=59, microsecond=117000),
        datetime.datetime(2020, 6, 25, hour=16, minute=34, second=2, microsecond=121000),
        datetime.datetime(2020, 6, 25, hour=16, minute=34, second=5, microsecond=125000),
        datetime.datetime(2020, 6, 25, hour=16, minute=34, second=8, microsecond=130000),
    ]

    for row in ax:
        for axes in row:
            for t in reconfigurations:
                axes.axvline(x=origin + (t - naive_start_time), color='black',
                             alpha=0.5)

    # Plot data.
    plot_latency(ax[0][0], 'No Opts', 'C0', baseline['latency_nanos'] / 1e6, sample_every)
    plot_latency(ax[0][1], 'Opt 3', 'C1', gc['latency_nanos'] / 1e6, sample_every)
    plot_latency(ax[0][2], 'Opt 2,3', 'C2', phase1['latency_nanos'] / 1e6, sample_every)
    plot_latency(ax[0][3], 'Opt 1,2,3', 'C3', matchmaking['latency_nanos'] / 1e6, sample_every)
    plot_throughput(ax[1][0], 'No Opts', 'C0', baseline['delta'], sample_every)
    plot_throughput(ax[1][1], 'Opt 3', 'C1', gc['delta'], sample_every)
    plot_throughput(ax[1][2], 'Opt 2,3', 'C2', phase1['delta'], sample_every)
    plot_throughput(ax[1][3], 'Opt 1,2,3', 'C3', matchmaking['delta'], sample_every)

    # Format x ticks nicely.
    for row in ax:
        for axes in row:
            axes.grid()
            # axes.legend(loc='best')
            axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%-M:%S'))
            for label in axes.get_xticklabels():
                # label.set_ha('left')
                label.set_ha('center')
                label.set_rotation(-90)

    # Write titles.
    ax[0][0].set_title('No optimizations')
    ax[0][1].set_title('Optimization 3')
    ax[0][2].set_title('Optimizations 2, 3')
    ax[0][3].set_title('Optimizations 1, 2, 3')

    # Save figures.
    for axes in ax[1]:
        axes.set_xlabel('Time')
    ax[0][0].set_ylabel('Latency (ms)')
    ax[1][0].set_ylabel('Throughput\n(cmds/second)')
    fig.set_tight_layout(True)
    # fig.savefig(output_filename, bbox_extra_artists=(legend,),
    #             bbox_inches='tight')
    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def main(args) -> None:
    # Read the data.
    (baseline, start_time) = read_data(args.baseline, args.drop_head,
                                       args.drop_tail + 5)
    (gc, _) = read_data(args.gc, args.drop_head, args.drop_tail)
    (phase1, _) = read_data(args.phase1, args.drop_head, args.drop_tail)
    (matchmaking, _) = read_data(args.matchmaking, args.drop_head,
                                 args.drop_tail)

    plot(
        baseline=baseline,
        gc=gc,
        phase1=phase1,
        matchmaking=matchmaking,
        output_filename=args.output,
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

    parser.add_argument('--baseline',
                        type=argparse.FileType('r'),
                        help='Baseline (no optimization) data.csv file')
    parser.add_argument('--gc',
                        type=argparse.FileType('r'),
                        help='+gc optimization data.csv file')
    parser.add_argument('--phase1',
                        type=argparse.FileType('r'),
                        help='+phase1 optimization data.csv file')
    parser.add_argument('--matchmaking',
                        type=argparse.FileType('r'),
                        help='+matchmaking optimization data.csv file')
    parser.add_argument('--output',
                        type=str,
                        default='ablation.pdf',
                        help='Output filename')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
