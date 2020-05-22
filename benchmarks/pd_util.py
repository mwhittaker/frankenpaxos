from typing import Iterable, List
import numpy as np
import pandas as pd


def read_csvs(filenames: Iterable[str], **kwargs) -> pd.DataFrame:
    """
    pd.read_csv reads in a _single_ CSV file and converts it to a dataframe.
    read_csvs reads in a _set_ of CSVs, concatenates them together, and
    converts the concatenation to a dataframe.
    """
    dfs: List[pd.DataFrame] = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename, header=0, **kwargs))
    return pd.concat(dfs, ignore_index=True)


def outliers(s: pd.Series, n: float) -> pd.Series:
    """
    `outliers(s, n)` is a boolean vector of the values in s that are n or more
    standard deviations away from the mean. You can use `outliers` to select
    outliers like this:

        s[outliers(s, n)]

    or prune outliers like this:

        s[~outliers(s, n)]
    """
    mu = s.mean()
    sigma = s.std()
    return np.abs(s - mu) >= (n * sigma)


def throughput(s: pd.Series, window_size_ms: float,
               trim: bool = False) -> pd.Series:
    """
    Consider a series of timestamps:

        timestamp
      0 11:00:01 am
      1 11:00:03 am
      2 11:00:54 am
      3 11:01:34 am
      4 11:02:16 am

    Imagine we divide the data into 1 minute rolling windows with every window
    having its right edge be a entry in the dataframe. We'd get the following
    windows and latencies:

                                  timestamps
      10:59:01 am - 11:00:01 am | [0]       |
      10:59:03 am - 11:00:03 am | [0, 1]    |
      10:59:54 am - 11:00:54 am | [0, 1, 2] |
      11:00:34 am - 11:01:34 am | [2, 3]    |
      11:01:16 am - 11:02:16 am | [2, 3, 4] |

    If we count the number of entries in each window and divide by the window
    size, we get the throughput of each window measured in events per second.

                                  throughput
      10:59:01 am - 11:00:01 am | 1 / 60     |
      10:59:03 am - 11:00:03 am | 2 / 60     |
      10:59:54 am - 11:00:54 am | 3 / 60     |
      11:00:34 am - 11:01:34 am | 2 / 60     |
      11:01:16 am - 11:02:16 am | 3 / 60     |

    This is what `throughput` computes. If `trim` is true, the first
    window_size_ms of throughput data is trimmed.
    """
    s = pd.Series(0, index=s.sort_values())
    throughput = (s.rolling(f'{window_size_ms}ms').count() /
                  (window_size_ms / 1000))
    if trim:
        t = (throughput.index[0] +
             pd.DateOffset(microseconds=window_size_ms * 1000))
        return throughput[throughput.index >= t]
    else:
        # TODO(mwhittaker): Fix up. It's a little jank.
        start_time = throughput.index[0]
        offset = pd.DateOffset(microseconds=window_size_ms*1000)
        for i, (index, row) in enumerate(s.iteritems(), start=1):
            if i < 100:
                continue
            if index > start_time + offset:
                return throughput[100:]
            throughput[index] = i / (index - start_time).total_seconds()
        return throughput[100:]


def weighted_throughput(s: pd.Series, window_size_ms: float) -> pd.Series:
    """
    weighted_throughput is the same as throughput, except that every
    measurement in `s` is weighted with some count. A measurement with count
    `x` is counted `x` times.
    """
    window_size_s = window_size_ms / 1000
    window_size_us = window_size_ms * 1000

    s = s.sort_index()
    throughput = s.rolling(f'{window_size_ms}ms').sum() / window_size_s
    t = throughput.index[0] + pd.DateOffset(microseconds=window_size_us)
    return throughput[throughput.index >= t]


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
    return (s.sort_index(0).rolling(f'{window_size_ms}ms',
                                    min_periods=2).apply(_dxdt, raw=False))
