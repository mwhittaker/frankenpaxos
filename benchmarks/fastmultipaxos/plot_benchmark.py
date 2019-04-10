from .. import parser_util
from .. import pd_util
from .. import plot_latency_and_throughput
from .. import plt_util
from typing import Callable, Dict, NamedTuple, List, Tuple
import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd


def plot_leaders(ax: plt.Axes,
                 p_df: pd.DataFrame,
                 num_leaders: int,
                 metrics: List[Tuple[str, str]],
                 f: Callable[[pd.Series], pd.Series]) -> None:
    instances = [(f'leader_{i}', f'Leader {i}') for i in range(num_leaders)]
    plt_util.plot_prometheus_data(ax, p_df, metrics, instances, f)


def plot_clients(ax: plt.Axes,
                 p_df: pd.DataFrame,
                 num_clients: int,
                 metrics: List[Tuple[str, str]],
                 f: Callable[[pd.Series], pd.Series]) -> None:
    instances = [(f'client_{i}', f'Client {i}') for i in range(num_clients)]
    plt_util.plot_prometheus_data(ax, p_df, metrics, instances, f)


def plot_leader_request_throughput(ax: plt.Axes,
                                   p_df: pd.DataFrame,
                                   num_leaders: int) -> None:
    pre = 'fast_multipaxos_leader'
    metrics = [
        (f'{pre}_requests_total_ProposeRequest', 'ProposeRequest'),
        (f'{pre}_requests_total_Phase1b', 'Phase1b'),
        (f'{pre}_requests_total_Phase1bNack', 'Phase1bNack'),
        (f'{pre}_requests_total_Phase2b', 'Phase2b'),
        (f'{pre}_requests_total_ValueChosen', 'ValueChosen'),
    ]
    f = lambda s: pd_util.rate(s, 1000)
    plot_leaders(ax, p_df, num_leaders, metrics, f)
    ax.set_title(f'Leader Request Throughput (1s windows)')
    ax.set_ylabel('Throughput')


def plot_leader_command_throughput(ax: plt.Axes,
                                   p_df: pd.DataFrame,
                                   num_leaders: int) -> None:
    pre = 'fast_multipaxos_leader'
    metrics = [
        (f'{pre}_executed_commands_total', 'Executed Commands'),
        (f'{pre}_executed_noops_total', 'Executed Noops'),
        (f'{pre}_repeated_commands_total', 'Repeated Commands'),
        (f'{pre}_chosen_commands_total_fast', 'Chosen Commands (Fast)'),
        (f'{pre}_chosen_commands_total_classic', 'Chosen Commands (Classic)'),
    ]
    f = lambda s: pd_util.rate(s, 1000)
    plot_leaders(ax, p_df, num_leaders, metrics, f)
    ax.set_ylabel('Throughput')
    ax.set_title(f'Leader Command Throughput (1s windows)')


def plot_leader_change_throughput(ax: plt.Axes,
                                  p_df: pd.DataFrame,
                                  num_leaders: int) -> None:
    pre = 'fast_multipaxos_leader'
    metrics = [
        (f'{pre}_leader_changes_total', 'Leader Changes'),
        (f'{pre}_stuck_total', 'Stuck'),
    ]
    f = lambda s: pd_util.rate(s, 1000)
    plot_leaders(ax, p_df, num_leaders, metrics, f)
    ax.set_title(f'Leader Change Throughput (1s windows)')
    ax.set_ylabel('Throughput')


def plot_client_throughput(ax: plt.Axes,
                           p_df: pd.DataFrame,
                           num_clients: int) -> None:
    pre = 'fast_multipaxos_client'
    metrics = [(f'{pre}_responses_total', '')]
    f = lambda s: pd_util.rate(s, 1000)
    plot_clients(ax, p_df, num_clients, metrics, f)
    ax.set_title(f'Client Throughput (1s windows)')
    ax.set_ylabel('Throughput')


def main(args) -> None:
    df, p_df = parser_util.plot_benchmark_parse(args)
    with open(args.input) as f:
        input = json.load(f)

    num_plots = 6
    fig, ax = plt.subplots(num_plots, 1, figsize=(10, num_plots * 4.8))
    ax_iter = iter(ax)

    plot_latency_and_throughput.plot_latency(next(ax_iter), df['latency_ms'])
    plot_latency_and_throughput.plot_throughput(next(ax_iter), df)
    plot_leader_request_throughput(next(ax_iter), p_df, input['f'] + 1)
    plot_leader_command_throughput(next(ax_iter), p_df, input['f'] + 1)
    plot_leader_change_throughput(next(ax_iter), p_df, input['f'] + 1)
    plot_client_throughput(next(ax_iter), p_df, input['num_clients'])

    for axes in ax:
        axes.legend(loc='center left', bbox_to_anchor=(1, 0.5))
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
