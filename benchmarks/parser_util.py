from . import pd_util
from typing import Tuple
import argparse
import os
import pandas as pd

def get_benchmark_parser() -> argparse.ArgumentParser:
    """
    get_benchmark_parser returns an argument parser with the flags most
    commonly used by benchmark scripts.
    """
    parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-s', '--suite_directory',
        type=str,
        default='/tmp',
        help='Benchmark suite directory'
    )
    parser.add_argument(
        '-j', '--jar',
        type=str,
        default = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            '..',
            'jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar'
        ),
        help='FrankenPaxos JAR file'
    )
    parser.add_argument(
        '-p', '--profile',
        action='store_true',
        help='Profile code using perf'
    )
    parser.add_argument(
        '-m', '--monitor',
        action='store_true',
        help='Monitor code using prometheus'
    )
    return parser


def get_plot_benchmark_parser() -> argparse.ArgumentParser:
    """
    get_plot_benchmark_parser returns an argument parser with the flags most
    commonly used by benchmark plotting scripts.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'data',
        type=str,
        help='Single benchmark CSV file (e.g., data.csv)',
    )
    parser.add_argument(
        'prometheus_data',
        type=str,
        help='Prometheus benchmark CSV file (e.g., prometheus_data.csv)',
    )
    parser.add_argument(
        '-d', '--drop',
        type=float,
        default=0,
        help='Drop this number of seconds from the beginning of the benchmark',
    )
    parser.add_argument(
        '-s', '--stds',
        type=float,
        default=1e20,
        help='Latencies that deviate by more than <stds> stds are stripped',
    )
    return parser


def plot_benchmark_parse(args: argparse.Namespace) \
                         -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parses data and prometheus_data using the arguments parsed by
    get_benchmark_parser.
    """
    # Read in data.
    df = pd.read_csv(args.data, parse_dates=['start', 'stop'])
    df.index = df['start']
    df = df.sort_index(0)

    p_df = pd.read_csv(args.prometheus_data, index_col=[0], parse_dates=[0])
    p_df = p_df.sort_index(0)

    # Drop first bit of data and end of prometheus data.
    start_time = df['start'].iloc[0]
    new_start_time = start_time + pd.DateOffset(seconds=args.drop)
    df = df[df['start'] >= new_start_time]
    p_df = p_df[p_df.index >= new_start_time]
    p_df = p_df[p_df.index <= df['stop'].iloc[-1]]

    # Strip outliers.
    latency_ms = df['latency_nanos'] / 1e6
    latency_ms = latency_ms[~pd_util.outliers(latency_ms, args.stds)]
    df['latency_ms'] = latency_ms

    return (df, p_df)
