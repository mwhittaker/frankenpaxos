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

    # Plot latency.
    if args.stds:
        lm = df['latency_millis']
        stripped = df[np.abs(lm - lm.mean()) <= (args.stds * lm.std())]
    else:
        stripped = df

    fig, ax = plt.subplots()
    lm = df['latency_millis']
    ax.plot_date(
        stripped.index,
        stripped['latency_millis'].rolling('100ms').mean(),
        label='100ms',
        fmt='-')
    ax.plot_date(
        stripped.index,
        stripped['latency_millis'].rolling('500ms').mean(),
        label='500ms',
        fmt='-')
    ax.plot_date(
        stripped.index,
        stripped['latency_millis'].rolling('1s').mean(),
        label='1s',
        fmt='-')
    ax.set_title('Latency')
    ax.set_xlabel('Time')
    ax.set_ylabel('Latency (ms)')
    ax.grid()
    ax.legend(loc='best')
    fig.set_tight_layout(True)
    filename = os.path.join(args.output, 'latency.pdf')
    fig.savefig(filename)
    print(f'Writing plot to {filename}.')

    # Plot throughput.
    fig, ax = plt.subplots()
    ax.plot_date(
        df.index,
        util.throughput(df, 100),
        label='100ms',
        fmt='-')
    ax.plot_date(
        df.index,
        util.throughput(df, 500),
        label='500ms',
        fmt='-')
    ax.plot_date(
        df.index,
        util.throughput(df, 1000),
        label='1s',
        fmt='-')
    ax.set_title('Throughput')
    ax.set_xlabel('Time')
    ax.set_ylabel('Throughput')
    ax.grid()
    ax.legend(loc='best')
    fig.set_tight_layout(True)
    filename = os.path.join(args.output, 'throughput.pdf')
    fig.savefig(filename)
    print(f'Writing plot to {filename}.')

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
        default='.',
        help='Output directory'
    )
    return parser

if __name__ == '__main__':
    main(get_parser().parse_args())
