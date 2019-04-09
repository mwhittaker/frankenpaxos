from .. import parser_util
from .. import pd_util
from .. import plot_latency_and_throughput
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd


def plot_throughput(ax: plt.Axes,
                    df: pd.DataFrame,
                    p_df: pd.DataFrame) -> None:
    def throughput_trimmed(df, ms):
        throughput = pd_util.throughput(df, ms)
        new_start = throughput.index[0] + pd.DateOffset(milliseconds=ms)
        return throughput[throughput.index >= new_start]

    ax.plot_date(pd_util.throughput(df, 250, trim=True).index,
                 pd_util.throughput(df, 250, trim=True),
                 label='250ms',
                 fmt='-',
                 alpha=0.5)
    ax.plot_date(pd_util.throughput(df, 500, trim=True).index,
                 pd_util.throughput(df, 500, trim=True),
                 label='500ms',
                 fmt='-',
                 alpha=0.7)
    ax.plot_date(pd_util.throughput(df, 1000, trim=True).index,
                 pd_util.throughput(df, 1000, trim=True),
                 label='1s',
                 fmt='-')
    prometheus_throughput = pd_util.rate(p_df['echo_requests_total'], 1000)
    ax.plot_date(prometheus_throughput.index,
                 prometheus_throughput,
                 label='1s (Prometheus)',
                 fmt='--')
    ax.set_title('Throughput')
    ax.set_xlabel('Time')
    ax.set_ylabel('Throughput')


def main(args) -> None:
    df, p_df = parser_util.plot_benchmark_parse(args)
    num_plots = 2
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))
    plot_latency_and_throughput.plot_latency(ax[0], df['latency_ms'])
    plot_throughput(ax[1], df, p_df)
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
    parser = parser_util.get_plot_benchmark_parser()
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='echo_benchmark.pdf',
        help='Output filename',
    )
    return parser


if __name__ == '__main__':
    from pandas.plotting import register_matplotlib_converters
    register_matplotlib_converters()
    main(get_parser().parse_args())
