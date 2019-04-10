from .. import parser_util
from .. import pd_util
from .. import plot_latency_and_throughput
import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd


def plot_leader_request_throughput(ax: plt.Axes,
                                   p_df: pd.DataFrame,
                                   num_leaders: int) -> None:
    styles = ['solid', 'dashed', 'dashdot', 'dotted']
    assert num_leaders <= len(styles)
    types = ["ProposeRequest", "Phase1b", "Phase1bNack", "Phase2b",
             "ValueChosen"]

    for type in types:
        color = None
        for (i, style) in zip(range(num_leaders), styles):
            metric_name = (f'fast_multipaxos_leader_requests_total' +
                           f'_{type}' +
                           f'_leader_{i}')
            if metric_name in p_df:
                s = p_df[metric_name].dropna()
                line = ax.plot_date(pd_util.rate(s, 1000).index,
                                    pd_util.rate(s, 1000),
                                    label=f'Leader {i} {type}',
                                    fmt='.',
                                    linestyle=style,
                                    color=color)[0]
                color = line.get_color()
    ax.set_title(f'Leader Request Throughput (1s windows)')
    ax.set_ylabel('Throughput')


def main(args) -> None:
    df, p_df = parser_util.plot_benchmark_parse(args)
    with open(args.input) as f:
        input = json.load(f)

    num_plots = 3
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))
    ax_iter = iter(ax)

    plot_latency_and_throughput.plot_latency(next(ax_iter), df['latency_ms'])
    plot_latency_and_throughput.plot_throughput(next(ax_iter), df)
    plot_leader_request_throughput(next(ax_iter), p_df, input['f'] + 1)

    for axes in ax:
        axes.legend(loc='best')
        axes.grid()
        for label in axes.get_xticklabels():
            label.set_ha('right')
            label.set_rotation(20)
    fig.set_tight_layout(True)
    fig.savefig(args.output)
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = parser_util.get_plot_benchmark_parser()
    parser.add_argument(
        'input',
        type=str,
        help='Input JSON file (e.g., input.json)',
    )
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
