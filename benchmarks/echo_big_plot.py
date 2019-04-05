import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

def plot_latency(df: pd.DataFrame, ax) -> None:
    grouped = df.groupby('num_clients').agg([np.mean, np.std])
    columns = {
        'mean_latency': 'mean',
        'median_latency': 'median',
        'p90_latency': 'P90',
        'p95_latency': 'P95',
        'p99_latency': 'P99',
    }
    for column in columns:
        mean = grouped[column]['mean'] / 1e6
        std = grouped[column]['std'] / 1e6
        line = ax.plot(mean, '.-', label=columns[column])[0]
        color = line.get_color()
        ax.fill_between(grouped.index, mean - std, mean + std, color=color,
                        alpha=0.25)

    ax.set_title('Latency')
    ax.set_xlabel('Number of clients')
    ax.set_ylabel('Latency (ms)')
    ax.grid()
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

def plot_1_second_throughput(df: pd.DataFrame, ax) -> None:
    grouped = df.groupby('num_clients').agg([np.mean, np.std])
    columns = {
        'mean_1_second_throughput': 'mean',
        'median_1_second_throughput': 'median',
        'p90_1_second_throughput': 'P90',
        'p95_1_second_throughput': 'P95',
        'p99_1_second_throughput': 'P99',
    }
    for column in columns:
        mean = grouped[column]['mean']
        std = grouped[column]['std']
        line = ax.plot(mean, '.-', label=columns[column])[0]
        color = line.get_color()
        ax.fill_between(grouped.index, mean - std, mean + std, color=color,
                        alpha=0.25)

    ax.set_title('Throughput (1 second windows)')
    ax.set_xlabel('Number of clients')
    ax.set_ylabel('Throughput')
    ax.grid()
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

def main(args) -> None:
    df = pd.read_csv(args.results_csv)

    # See [1] for figure size defaults. We add an extra plot at the bottom for
    # a textual note.
    #
    # [1]: https://matplotlib.org/api/_as_gen/matplotlib.pyplot.figure.html
    num_plots = 2
    fig, ax = plt.subplots(num_plots, 1, figsize=(6.4, num_plots * 4.8))

    plot_latency(df, ax[0])
    plot_1_second_throughput(df, ax[1])

    fig.set_tight_layout(True)
    fig.savefig(args.output)
    print(f'Wrote plot to {args.output}.')

def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'results_csv',
        type=str,
        help='results.csv file'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='echo_big.pdf',
        help='Output filename.'
    )
    return parser

if __name__ == '__main__':
    main(get_parser().parse_args())
