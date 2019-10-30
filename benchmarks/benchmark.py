# This file contains utilities for running benchmarks and collections of
# benchmarks (called benchmark suites).
#
# We view a benchmark as a function `f(input) -> output` that takes in some
# inputs and spits out some ouputs. For example, a prime factorization
# benchmark might take in a number `x` and output the time it takes to prime
# factorize `x`.
#
# A benchmark suite is a collection of inputs that are all passed to the same
# benchmark. For example, we might have one benchmark suite that passes small
# prime numbers to the prime factorization benchmark, and we might have another
# suite that passes large prime numbers to the prime factorization benchmark.
#
# This file contains utilities for running and organizing benchmarks suites.

from . import host
from . import pd_util
from . import proc
from . import util
from typing import (Any, Collection, Dict, Generic, Iterable, IO, List,
                    NamedTuple, Optional, Sequence, Tuple, TypeVar, Union)
import colorful
import contextlib
import csv
import datetime
import datetime
import json
import os
import pandas as pd
import random
import string
import subprocess


def _random_string(n: int) -> str:
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(n))


def _now_string() -> str:
    return str(datetime.datetime.now()).replace(' ', '_')


def _pretty_now_string() -> str:
    return datetime.datetime.now().strftime('%A %B %d, %H:%M:%S.%f')


class _Reaped(object):
    """
    The _Reaped context manager ensures that a process is killed, even if an
    exception is thrown. Moreover, the return code of the process is written to
    a file.
    """
    def __init__(self, proc: proc.Proc, returncode_file: str) -> None:
        self.proc = proc
        self.returncode_file = returncode_file

    def __enter__(self) -> proc.Proc:
        return self.proc

    def __exit__(self, cls, exn, traceback) -> None:
        self.proc.kill()
        returncode = self.proc.wait()
        with open(self.returncode_file, 'w') as f:
            f.write(str(returncode) + '\n')


# A SuiteDirectory is a directory in which you can run a suite. It has
# convenient methods to record information within the directory (e.g., the
# start time, the set of inputs). It also contains methods to create
# subdirectories for each benchmark in the suite.
class SuiteDirectory(object):
    def __init__(self, path: str, name: str = None) -> None:
        assert os.path.exists(path)

        self.benchmark_dir_id = 1

        name_suffix = ("_" + name) if name else ""
        self.path = os.path.join(
            os.path.abspath(path),
            _now_string() + '_' + _random_string(10) + name_suffix)
        assert not os.path.exists(self.path)
        os.makedirs(self.path)

    def __str__(self) -> str:
        return f'SuiteDirectory({self.path})'

    def __enter__(self):
        self.write_string('start_time.txt', str(datetime.datetime.now()))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.write_string('stop_time.txt', str(datetime.datetime.now()))

    def abspath(self, filename: str) -> str:
        return os.path.join(self.path, filename)

    def create_file(self, filename: str) -> IO:
        return open(self.abspath(filename), 'w')

    def write_string(self, filename: str, s: str) -> str:
        with self.create_file(filename) as f:
            f.write(s + '\n')
        return self.abspath(filename)

    def write_dict(self, filename: str, d: Dict) -> str:
        self.write_string(filename, json.dumps(d, indent=4, default=str))
        return self.abspath(filename)

    def benchmark_directory(self, name: str = None) -> 'BenchmarkDirectory':
        benchmark_dir_id = self.benchmark_dir_id
        self.benchmark_dir_id += 1
        name_suffix = ("_" + name) if name else ""
        path = os.path.join(self.path, "{:03}{}".format(benchmark_dir_id,
                                                        name_suffix))
        return BenchmarkDirectory(path)


# A BenchmarkDirectory is like a SuiteDirectory. It provides methods to record
# information about a benchmark as well as other helpful methods. For example,
# the popen method allows you to run an executable and record its standard out,
# standard error, and return code within a benchmark directory.
class BenchmarkDirectory(object):
    def __init__(self, path: str) -> None:
        assert not os.path.exists(path)
        self.path = os.path.abspath(path)
        os.makedirs(self.path)

        # We want to ensure that all processes run within a benchmark are
        # terminated if the benchmark is killed. Thus, we put all processes in
        # this stack.
        self.process_stack = contextlib.ExitStack()

        # A mapping from (ip address, pid) to the process running at that pid
        # on that ip address. Recording pids helps debug perf.
        self.pids: Dict[Tuple[str, int], str] = dict()

        # A file for logging.
        self.logfile = self.create_file('log.txt')

    def __str__(self) -> str:
        return f'BenchmarkDirectory({self.path})'

    def __enter__(self):
        self.process_stack.__enter__()
        self.write_string('start_time.txt', str(datetime.datetime.now()))
        return self

    def __exit__(self, cls, exn, trace):
        self.process_stack.__exit__(cls, exn, trace)
        self.write_dict(
            'pids.json',
            {f'{ip}:{pid}': label for ((ip, pid), label) in self.pids.items()})
        self.write_string('stop_time.txt', str(datetime.datetime.now()))

    def abspath(self, filename: str) -> str:
        return os.path.join(self.path, filename)

    def create_file(self, filename: str) -> IO:
        return open(self.abspath(filename), 'w')

    def write_string(self, filename: str, s: str) -> str:
        with self.create_file(filename) as f:
            f.write(s + '\n')
        return self.abspath(filename)

    def write_dict(self, filename: str, d: Dict) -> str:
        self.write_string(filename, json.dumps(d, indent=4, default=str))
        return self.abspath(filename)

    def log(self, s: str) -> None:
        self.logfile.write(f'[{_pretty_now_string()}] {s}\n')
        self.logfile.flush()

    def popen(self, host: host.Host, label: str,
              cmd: Union[str, Sequence[str]]) -> proc.Proc:
        """Runs a command within this directory.

        `popen` runs a command, recording the command, its stdout, its stderr,
        and its return code within the benchmark directory. For example,

            bench.popen(host, 'ls', ['ls', '-l'])

        runs `ls -l`. The string `ls -l` is written to `ls_cmd.txt`. The stdout
        and stderr of `ls -l` are written to `ls_out.txt` and `ls_err.txt`
        respectively. The return code of `ls -l` is return to
        `ls_returncode.txt`.
        """
        proc = host.popen(cmd,
                          stdout=self.abspath(f'{label}_out.txt'),
                          stderr=self.abspath(f'{label}_err.txt'))
        self.write_string(f'{label}_cmd.txt', proc.cmd())
        self.process_stack.enter_context(
            _Reaped(proc, self.abspath(f'{label}_returncode.txt')))
        pid = proc.pid()
        if pid:
            self.pids[(host.ip(), pid)] = label
        return proc


# A Suite represents a benchmark suite. A suite is parameterized on an input
# type Input and output type Output. A suite must provide
#
#  - a set of global suite arguments using the method `args`,
#  - a list of benchmark inputs using the method `inputs`,
#  - a `summary` function to summarize a benchmark results for printing, and
#  - a `run_benchmark` function to run a benchmark.
Input = TypeVar('Input')
Output = TypeVar('Output')


class Suite(Generic[Input, Output]):
    def args(self) -> Dict[Any, Any]:
        raise NotImplementedError("")

    def inputs(self) -> Collection[Input]:
        raise NotImplementedError("")

    def summary(self, input: Input, output: Output) -> str:
        raise NotImplementedError("")

    def run_benchmark(self, bench: BenchmarkDirectory, args: Dict[Any, Any],
                      input: Input) -> Output:
        raise NotImplementedError("")

    def run_suite(self, suite_dir: SuiteDirectory) -> None:
        print(f'Running suite in {suite_dir.path}.')

        # Sanity check args and inputs.
        args = self.args()
        inputs = self.inputs()
        assert len(inputs) > 0, inputs

        # Record args and inputs.
        suite_dir.write_dict('args.json', args)
        suite_dir.write_string('inputs.txt', '\n'.join(str(i) for i in inputs))

        # Create file to record suite results.
        results_file = suite_dir.create_file('results.csv')
        results_writer = csv.writer(results_file)

        suite_start_time = datetime.datetime.now()
        for (i, input) in enumerate(inputs, 1):
            bench_start_time = datetime.datetime.now()
            with suite_dir.benchmark_directory() as bench:
                # Run the benchmark.
                bench.write_string('input.txt', str(input))
                bench.write_dict('input.json', util.tuple_to_dict(input))
                output = self.run_benchmark(bench, args, input)

                # Write the header if needed.
                if i == 1:
                    results_writer.writerow(
                        util.flatten_tuple_fields(input) +
                        util.flatten_tuple_fields(output))

                # Write the results.
                row = util.flatten_tuple(input) + util.flatten_tuple(output)
                results_writer.writerow([str(x) for x in row])
                results_file.flush()

            # Display some information about the benchmark.
            colorful.use_style('monokai')

            #First, we show the progress of the suite.
            n = len(inputs)
            percent = (i / n) * 100
            info = f'{colorful.bold}[{i:03}/{n:03}{colorful.reset}; '
            info += f'{percent:#.4}%] '

            # Next, we show the time taken to run this benchmark, the total
            # elapsed time, and the estimated time left.
            current_time = datetime.datetime.now()
            bench_duration = current_time - bench_start_time
            suite_duration = current_time - suite_start_time
            duration_per_iteration = suite_duration / i
            remaining_duration = (n - i) * duration_per_iteration

            def round_timedelta(d):
                return datetime.timedelta(seconds=int(d.total_seconds()))

            info += f'{colorful.blue(round_timedelta(bench_duration))} / '
            info += f'{colorful.green(round_timedelta(suite_duration))} + '
            info += f'{colorful.magenta(round_timedelta(remaining_duration))}? '

            # Finally, we display a summary of the benchmark.
            info += f'{colorful.lightGray(self.summary(input, output))}'
            print(info)


class LatencyOutput(NamedTuple):
    mean_ms: float
    median_ms: float
    min_ms: float
    max_ms: float
    p90_ms: float
    p95_ms: float
    p99_ms: float


class ThroughputOutput(NamedTuple):
    mean: float
    median: float
    min: float
    max: float
    p90: float
    p95: float
    p99: float


class RecorderOutput(NamedTuple):
    latency: LatencyOutput
    start_throughput_1s: ThroughputOutput
    start_throughput_2s: ThroughputOutput
    start_throughput_5s: ThroughputOutput
    stop_throughput_1s: ThroughputOutput
    stop_throughput_2s: ThroughputOutput
    stop_throughput_5s: ThroughputOutput


# parse_recorder_data parses and summarizes data written by a
# frankenpaxos.BenchmarkUtil.Recorder.
#
# TODO(mwhittaker): Drop the first couple of seconds from the data since it
# takes a while for the JVM to fully ramp up.
def parse_recorder_data(bench: BenchmarkDirectory, filenames: Iterable[str],
                        drop_prefix: datetime.timedelta, save_data: bool = True) -> RecorderOutput:
    df = pd_util.read_csvs(filenames, parse_dates=['start', 'stop'])
    bench.log('Aggregate recorder data read.')
    df = df.set_index('start')
    bench.log('Aggregate recorder data index set.')
    df = df.sort_index(0)
    bench.log('Aggregate recorder data index sorted.')
    if save_data:
        df.to_csv(bench.abspath('data.csv'))
        bench.log('Aggregate recorder data written.')

    # Since we concatenate and save the file, we can throw away the originals.
    for filename in filenames:
        os.remove(filename)
    bench.log('Individual recorder data removed.')

    # We also compress the output data since it can get big.
    if save_data:
        subprocess.call(['gzip', bench.abspath('data.csv')])
        bench.log('Aggregate recorder data compressed.')

    # Drop prefix of data.
    start_time = df.index[0]
    new_start_time = (start_time +
                      pd.DateOffset(seconds=drop_prefix.total_seconds()))
    df = df[df.index >= new_start_time]

    def latency(s):
        return LatencyOutput(
            mean_ms=s.mean(),
            median_ms=s.median(),
            min_ms=s.min(),
            max_ms=s.max(),
            p90_ms=s.quantile(.90),
            p95_ms=s.quantile(.95),
            p99_ms=s.quantile(.99),
        )

    def throughput(s):
        return ThroughputOutput(
            mean=s.mean(),
            median=s.median(),
            min=s.min(),
            max=s.max(),
            p90=s.quantile(.90),
            p95=s.quantile(.95),
            p99=s.quantile(.99),
        )

    return RecorderOutput(
        latency=latency(df['latency_nanos'] / 1e6),
        start_throughput_1s=throughput(pd_util.throughput(df.index, 1000)),
        start_throughput_2s=throughput(pd_util.throughput(df.index, 2000)),
        start_throughput_5s=throughput(pd_util.throughput(df.index, 5000)),
        stop_throughput_1s=throughput(pd_util.throughput(df['stop'], 1000)),
        stop_throughput_2s=throughput(pd_util.throughput(df['stop'], 2000)),
        stop_throughput_5s=throughput(pd_util.throughput(df['stop'], 5000)),
    )
