from .. import plot_latency_and_throughput
from .. import parser_util
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd


def main(args) -> None:
    df, p_df = parser_util.plot_benchmark_parse(args)
    num_plots = 2
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))
    plot_latency_and_throughput.plot_latency(ax[0], df['latency_ms'])
    # plot_throughput(ax[1], df, p_df)
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
        default='fastmultipaxos_benchmark.pdf',
        help='Output filename',
    )
    return parser


if __name__ == '__main__':
    from pandas.plotting import register_matplotlib_converters
    register_matplotlib_converters()
    main(get_parser().parse_args())
