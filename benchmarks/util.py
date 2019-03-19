from typing import List
import contextlib
import pandas as pd
import subprocess

# Imagine you have the following python code in a file called sleep.py. The
# code creates a subprocess and waits for it to terminate.
#
#   p = subprocess.Popen(['sleep', '100'])
#   p.wait()
#
# If you run `python sleep.py` and then kill the process before it terminates,
# sometimes the subprocess lives on. The Reaped context manager ensures that
# the process is killed, even if an exception is thrown.
class Reaped(object):
    def __init__(self, p: subprocess.Popen) -> None:
        self.p = p

    def __enter__(self) -> subprocess.Popen:
        return self.p

    def __exit__(self, cls, exn, traceback) -> None:
        self.p.terminate()

# pd.read_csv reads in a CSV file and converts it to a dataframe. read_csvs
# reads in a _set_ of CSVs, concatenates them together, and converts the
# concatenation to a dataframe.
def read_csvs(filenames: List[str]) -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename, header=0))
    return pd.concat(dfs, ignore_index=True)

# Consider a timestamp indexed dataframe:
#
#                 latency
#   11:00:01 am | 100     |
#   11:00:03 am | 200     |
#   11:00:54 am | 300     |
#   11:01:34 am | 400     |
#   11:02:16 am | 500     |
#
# Imagine we divide the data into 1 minute rolling windows with every window
# having its right edge be a entry in the dataframe. We'd get the following
# windows and latencies:
#
#                               latencies
#   10:59:01 am - 11:00:01 am | [100]           |
#   10:59:03 am - 11:00:03 am | [100, 200]      |
#   10:59:54 am - 11:00:54 am | [100, 200, 300] |
#   11:00:34 am - 11:01:34 am | [300, 400]      |
#   11:01:16 am - 11:02:16 am | [300, 400, 500] |
#
# If we count the number of entries in each window and divide by the window
# size, we get the throughput of each window measured in events per second.
#
#                               throughput
#   10:59:01 am - 11:00:01 am | 1 / 60     |
#   10:59:03 am - 11:00:03 am | 2 / 60     |
#   10:59:54 am - 11:00:54 am | 3 / 60     |
#   11:00:34 am - 11:01:34 am | 2 / 60     |
#   11:01:16 am - 11:02:16 am | 3 / 60     |
#
# This is what `throughput` computes.
def throughput(df: pd.DataFrame, window_size_ms: float) -> pd.Series:
    s = pd.Series(0, index=df.sort_index(0).index)
    return s.rolling(f'{window_size_ms}ms').count() / (window_size_ms / 1000)
