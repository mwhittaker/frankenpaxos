from .. import parser_util
from .. import pd_util
from .. import plot_latency_and_throughput
import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

# TODO(mwhittaker): Combine leader plots into one. There's not many leaders.
def plot_leader_request_throughput(ax: plt.Axes,
                                   p_df: pd.DataFrame,
                                   leader_id: int) -> None:
    for type in ["ProposeRequest", "Phase1b", "Phase1bNack", "Phase2b",
                 "ValueChosen"]:
        metric_name = (f'fast_multipaxos_leader_requests_total' +
                       f'_{type}' +
                       f'_leader_{leader_id}')
        if metric_name in p_df:
            s = p_df[metric_name].dropna()
            ax.plot_date(pd_util.rate(s, 1000).index,
                         pd_util.rate(s, 1000),
                         label=f'{type} (1s)',
                         fmt='-')
            ax.set_title(f'Leader {leader_id} Throughput')
            ax.set_ylabel('Throughput')


# TODO(mwhittaker): Make a plot per leader with these counters:
#
# .name("fast_multipaxos_leader_executed_commands_total")
# .name("fast_multipaxos_leader_executed_noops_total")
# .name("fast_multipaxos_leader_repeated_commands_total")
# .name("fast_multipaxos_leader_chosen_commands_total")
# .labelNames("type") // "fast" or "classic".

# def plot_leader_execution_throughput(
#     ax: plt.Axes,
#     p_df: pd.DataFrame,
#     leader_id: int
# ) -> None:
#         metric_name = (f'fast_multipaxos_leader_requests_total' +
#                        f'_{type}' +
#                        f'_leader_{leader_id}')
#         if metric_name in p_df:
#             s = p_df[metric_name].dropna()
#             ax.plot_date(pd_util.rate(s, 1000).index,
#                          pd_util.rate(s, 1000),
#                          label=f'{type} (1s)',
#                          fmt='-')
#             ax.set_title(f'Leader {leader_id} Throughput')
#             ax.set_ylabel('Throughput')

# def plot_throughput(ax: plt.Axes,
#                     df: pd.DataFrame,
#                     p_df: pd.DataFrame) -> None:
#     ax.plot_date(pd_util.throughput(df, 250, trim=True).index,
#                  pd_util.throughput(df, 250, trim=True),
#                  label='250ms',
#                  fmt='-',
#                  alpha=0.5)
#     ax.plot_date(pd_util.throughput(df, 500, trim=True).index,
#                  pd_util.throughput(df, 500, trim=True),
#                  label='500ms',
#                  fmt='-',
#                  alpha=0.7)
#     ax.plot_date(pd_util.throughput(df, 1000, trim=True).index,
#                  pd_util.throughput(df, 1000, trim=True),
#                  label='1s',
#                  fmt='-')
#     # prometheus_throughput = pd_util.rate(p_df['fast_multipaxos_leader'], 1000)
#     # ax.plot_date(prometheus_throughput.index,
#     #              prometheus_throughput,
#     #              label='1s (Prometheus)',
#     #              fmt='--')
#     ax.set_title('Throughput')
#     ax.set_xlabel('Time')
#     ax.set_ylabel('Throughput')
#

def main(args) -> None:
    df, p_df = parser_util.plot_benchmark_parse(args)
    with open(args.input) as f:
        input = json.load(f)
    num_leaders = input['f'] + 1

    num_plots = 2 + num_leaders
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))
    ax_iter = iter(ax)

    plot_latency_and_throughput.plot_latency(next(ax_iter), df['latency_ms'])
    plot_latency_and_throughput.plot_throughput(next(ax_iter), df)
    for i in range(num_leaders):
        plot_leader_request_throughput(next(ax_iter), p_df, i)

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
