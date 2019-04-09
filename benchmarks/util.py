from typing import List
import argparse
import contextlib
import numpy as np
import os
import pandas as pd
import subprocess


def get_parser() -> argparse.ArgumentParser:
    """Returns an argument parser with the most commonly used flags."""
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


class Reaped(object):
    """
    Imagine you have the following python code in a file called sleep.py. The
    code creates a subprocess and waits for it to terminate.

      p = subprocess.Popen(['sleep', '100'])
      p.wait()

    If you run `python sleep.py` and then kill the process before it terminates,
    sometimes the subprocess lives on. The Reaped context manager ensures that
    the process is killed, even if an exception is thrown.
    """
    def __init__(self, p: subprocess.Popen) -> None:
        self.p = p

    def __enter__(self) -> subprocess.Popen:
        return self.p

    def __exit__(self, cls, exn, traceback) -> None:
        self.p.terminate()


def read_csvs(filenames: List[str], **kwargs) -> pd.DataFrame:
    """
    pd.read_csv reads in a CSV file and converts it to a dataframe. read_csvs
    reads in a _set_ of CSVs, concatenates them together, and converts the
    concatenation to a dataframe.
    """
    dfs: List[pd.DataFrame] = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename, header=0, **kwargs))
    return pd.concat(dfs, ignore_index=True)


def outliers(s: pd.Series, n: float) -> pd.Series:
    mu = s.mean()
    sigma = s.std()
    return np.abs(s - mu) >= (n * sigma)


def throughput(df: pd.DataFrame, window_size_ms: float) -> pd.Series:
    """
    Consider a timestamp indexed dataframe:

                    latency
      11:00:01 am | 100     |
      11:00:03 am | 200     |
      11:00:54 am | 300     |
      11:01:34 am | 400     |
      11:02:16 am | 500     |

    Imagine we divide the data into 1 minute rolling windows with every window
    having its right edge be a entry in the dataframe. We'd get the following
    windows and latencies:

                                  latencies
      10:59:01 am - 11:00:01 am | [100]           |
      10:59:03 am - 11:00:03 am | [100, 200]      |
      10:59:54 am - 11:00:54 am | [100, 200, 300] |
      11:00:34 am - 11:01:34 am | [300, 400]      |
      11:01:16 am - 11:02:16 am | [300, 400, 500] |

    If we count the number of entries in each window and divide by the window
    size, we get the throughput of each window measured in events per second.

                                  throughput
      10:59:01 am - 11:00:01 am | 1 / 60     |
      10:59:03 am - 11:00:03 am | 2 / 60     |
      10:59:54 am - 11:00:54 am | 3 / 60     |
      11:00:34 am - 11:01:34 am | 2 / 60     |
      11:01:16 am - 11:02:16 am | 3 / 60     |

    This is what `throughput` computes.
    """
    s = pd.Series(0, index=df.sort_index(0).index)
    return s.rolling(f'{window_size_ms}ms').count() / (window_size_ms / 1000)


def rate(s: pd.Series, window_size_ms: float) -> pd.Series:
    """
    Consider a series of data `s` that looks like this:

        | 12:00:01.00 | 10  |
        | 12:00:01.25 | 20  |
        | 12:00:01.50 | 30  |
        | 12:00:02.50 | 100 |

    `rate(s, n)` returns the rate of change of the value in `s` computed over
    sliding windows of `n` milliseconds. Every window's right boundary is a
    value in `s`. In this example, `rate(s, 500)` returns the following:

        | 12:00:01.00 | na | # Only one point in window.
        | 12:00:01.25 | 40 | # (20 - 10) / 0.25 s
        | 12:00:01.50 | 40 | # (30 - 10) / 0.5 s
        | 12:00:02.50 | na | # Only one point in wndow.

    And `rate(s, 1000)` returns the following

        | 12:00:01.00 | na | # Only one point in window.
        | 12:00:01.25 | 40 | # (20 - 10) / 0.25 s
        | 12:00:01.50 | 40 | # (30 - 10) / 0.5 s
        | 12:00:02.50 | 70 | # (100 - 30) / 1s
    """
    def _dxdt(s: pd.Series) -> float:
        dx = s.iloc[-1] - s.iloc[0]
        dt = (s.index[-1] - s.index[0]).total_seconds()
        return dx / dt

    # We set min_periods=2 because if we only have one data point in a window,
    # the rate in the window is ill defined.
    return (s.sort_index(0)
             .rolling(f'{window_size_ms}ms', min_periods=2)
             .apply(_dxdt, raw=False))
