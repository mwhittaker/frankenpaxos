from . import util
import numpy as np
import argparse
import matplotlib.pyplot as plt
import os
import pandas as pd

def main(args) -> None:
    df = pd.read_csv(args.data_csv)
    df['start'] = pd.to_datetime(df['start'])
    df['stop'] = pd.to_datetime(df['stop'])
    df['latency_millis'] = df['latency_nanos'] / 1e6
    df = df.set_index('start').sort_index(0)

    # See [1] for figure size defaults.
    #
    # [1]: https://matplotlib.org/api/_as_gen/matplotlib.pyplot.figure.html
    fig, ax = plt.subplots(2, 1, figsize=(6.4, 2 * 4.8))

    # Plot latency.
    if args.stds:
        lm = df['latency_millis']
        stripped = df[np.abs(lm - lm.mean()) <= (args.stds * lm.std())]
    else:
        stripped = df

    lm = df['latency_millis']
    ax[0].plot_date(
        stripped.index,
        stripped['latency_millis'].rolling('100ms').mean(),
        label='100ms',
        fmt='-')
    ax[0].plot_date(
        stripped.index,
        stripped['latency_millis'].rolling('500ms').mean(),
        label='500ms',
        fmt='-')
    ax[0].plot_date(
        stripped.index,
        stripped['latency_millis'].rolling('1s').mean(),
        label='1s',
        fmt='-')
    ax[0].set_title('Latency')
    ax[0].set_xlabel('Time')
    ax[0].set_ylabel('Latency (ms)')
    ax[0].grid()
    ax[0].legend(loc='best')

    # Plot throughput.
    ax[1].plot_date(
        df.index,
        util.throughput(df, 100),
        label='100ms',
        fmt='-')
    ax[1].plot_date(
        df.index,
        util.throughput(df, 500),
        label='500ms',
        fmt='-')
    ax[1].plot_date(
        df.index,
        util.throughput(df, 1000),
        label='1s',
        fmt='-')
    ax[1].set_title('Throughput')
    ax[1].set_xlabel('Time')
    ax[1].set_ylabel('Throughput')
    ax[1].grid()
    ax[1].legend(loc='best')

    # Save figure.
    fig.set_tight_layout(True)
    fig.savefig(args.output)
    print(f'Writing plot to {args.output}.')

def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'data_csv',
        type=str,
        help='data.csv file'
    )
    parser.add_argument(
        '-s', '--stds',
        type=float,
        default=None,
        help='Latency values that deviate by more that <stds> stds are stripped'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='latency_and_throughput.pdf',
        help='Output filename'
    )
    return parser

if __name__ == '__main__':
    main(get_parser().parse_args())
