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
import queue
import random
import string
import subprocess
import threading


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

        # Whether we have already exited. We want to avoid exiting twice.
        self.exited = False

    def __str__(self) -> str:
        return f'BenchmarkDirectory({self.path})'

    def __enter__(self):
        self.process_stack.__enter__()
        self.write_string('start_time.txt', str(datetime.datetime.now()))
        return self

    def __exit__(self, cls, exn, trace):
        if self.exited:
            return

        print(f'BenchmarkDirectory {self.path} exiting.')
        self.process_stack.__exit__(cls, exn, trace)
        self.exited = True
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
    # `args` returns a set of global arguments, typically passed in via the
    # command line.
    def args(self) -> Dict[Any, Any]:
        raise NotImplementedError("")

    # `inputs` returns the set of benchmark inputs that will run as part of
    # this suite.
    def inputs(self) -> Collection[Input]:
        raise NotImplementedError("")

    # As a suite runs, the results of the benchmarks are printed to the screen.
    # Printing the entire output, though, can be hard to read. So, before we
    # print an output, we first pass it to the `summary` function for pretty
    # printing. Different suites may want to print out different information,
    # depending on what exactly is being tested.
    def summary(self, input: Input, output: Output) -> str:
        raise NotImplementedError("")

    # `run_benchmark` takes in an Input and spits out an Output. It runs a
    # single benchmark to completion.
    def run_benchmark(self, bench: BenchmarkDirectory, args: Dict[Any, Any],
                      input: Input) -> Output:
        raise NotImplementedError("")

    # Often times, a benchmark consists of two phases. In Phase 1, the
    # benchmark runs some distributed code and collects some data. For example,
    # run Paxos and record all the command latencies. In Phase 2, it computes
    # summary statistics over the collected data. For example, compute the
    # median, mean, and max latency. As the data collected can be quite large,
    # Phase 2 can take a non-trivial amount of time. Thus, running both Phase 1
    # and Phase 2 serially is inefficient. While we are running Phase 2 of one
    # experiment, we can start running Phase 1 of the next experiment.
    #
    # The `run_remote_benchmark` and `run_local_benchmark` functions allow us
    # to do this. If we call `run_multithreaded_suite`, then these two
    # functions are run on two separate threads with queues connecting them.
    # This forms a pipeline that looks something like this:
    #
    #   inputs ---> run_remote_benchmark ---> run_local_benchmark ---> output
    def run_remote_benchmark(self,
                             suite: SuiteDirectory,
                             bench: BenchmarkDirectory,
                             args: Dict[Any, Any],
                             input: Input) -> Any:
        raise NotImplementedError("")

    # Here, `data` is the value produced by run_remote_benchmark.
    def run_local_benchmark(self,
                            suite: SuiteDirectory,
                            bench: BenchmarkDirectory,
                            args: Dict[Any, Any],
                            input: Input,
                            data: Any) -> Output:
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

            # First, we show the progress of the suite.
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

            def round_delta(d):
                return datetime.timedelta(seconds=int(d.total_seconds()))

            info += f'{colorful.blue(round_delta(bench_duration))} / '
            info += f'{colorful.green(round_delta(suite_duration))} + '
            info += f'{colorful.magenta(round_delta(remaining_duration))}? '

            # Finally, we display a summary of the benchmark.
            info += f'{colorful.lightGray(self.summary(input, output))}'
            print(info)

    class _RemoteInput(NamedTuple):
        bench_stack: contextlib.ExitStack
        suite: SuiteDirectory
        args: Dict[Any, Any]
        input: Any # Really, Input

    class _LocalInput(NamedTuple):
        suite: SuiteDirectory
        bench: BenchmarkDirectory
        args: Dict[Any, Any]
        input: Any
        bench_start_time: datetime.datetime
        data: Any

    class _Output(NamedTuple):
        input: Any # Really, Input
        output: Any # Really, Output
        bench_duration: datetime.timedelta

    def _run_remote_benchmarks(self,
                               inbox: queue.Queue, outbox: queue.Queue) -> None:
        while True:
            # Fetch data from the queue. If we receive a None, that's a signal
            # that we're out of work to do.
            in_data = inbox.get()
            if in_data is None:
                outbox.put(None)
                return

            # Create the benchmark directory.
            bench_start_time = datetime.datetime.now()
            bench = in_data.suite.benchmark_directory()
            bench.__enter__()
            in_data.bench_stack.enter_context(bench)

            # Run the benchmark.
            bench.write_string('input.txt', str(in_data.input))
            bench.write_dict('input.json', util.tuple_to_dict(in_data.input))
            data = self.run_remote_benchmark(in_data.suite,
                                             bench,
                                             in_data.args,
                                             in_data.input)

            # Relay the data on to the next phase of the pipeline.
            out_data = Suite._LocalInput(suite = in_data.suite,
                                         bench = bench,
                                         args = in_data.args,
                                         input = in_data.input,
                                         bench_start_time = bench_start_time,
                                         data = data)
            outbox.put(out_data)

    def _run_local_benchmarks(self,
                              inbox: queue.Queue, outbox: queue.Queue) -> None:
        while True:
            # Fetch data from the queue. If we receive a None, that's a signal
            # that we're out of work to do.
            in_data = inbox.get()
            if in_data is None:
                outbox.put(None)
                return

            # Run the benchmark.
            output = self.run_local_benchmark(suite = in_data.suite,
                                              bench = in_data.bench,
                                              args = in_data.args,
                                              input = in_data.input,
                                              data = in_data.data)
            in_data.bench.__exit__(None, None, None)

            # Relay the output to the next phase of the pipeline.
            now = datetime.datetime.now()
            out_data = Suite._Output(
                input = in_data.input,
                output = output,
                bench_duration = now - in_data.bench_start_time
            )
            outbox.put(out_data)

    def run_multithreaded_suite(self, suite_dir: SuiteDirectory) -> None:
        print(f'Running multithreaded suite in {suite_dir.path}.')

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

        # Start the threads.
        remote_in: queue.Queue[Optional[Suite._RemoteInput]] = queue.Queue()
        remote_out: queue.Queue[Optional[Suite._LocalInput]] = queue.Queue()
        local_out: queue.Queue[Optional[Suite._Output]] = queue.Queue()
        remote_thread = threading.Thread(target=self._run_remote_benchmarks,
                                         args=(remote_in, remote_out),
                                         daemon=True)
        local_thread = threading.Thread(target=self._run_local_benchmarks,
                                        args=(remote_out, local_out),
                                        daemon=True)
        remote_thread.start()
        local_thread.start()

        # Fill the pipeline.
        with contextlib.ExitStack() as bench_stack:
            suite_start_time = datetime.datetime.now()
            for input in inputs:
                remote_in.put(Suite._RemoteInput(
                    bench_stack = bench_stack,
                    suite = suite_dir,
                    args = args,
                    input = input,
                ))
            remote_in.put(None)

            # Pull from the pipeline.
            i = 0
            while True:
                out = local_out.get()
                if out is None:
                    break

                # Write the header if needed.
                i += 1
                if i == 1:
                    results_writer.writerow(
                        util.flatten_tuple_fields(out.input) +
                        util.flatten_tuple_fields(out.output))

                # Write the results.
                row = (util.flatten_tuple(out.input) +
                       util.flatten_tuple(out.output))
                results_writer.writerow([str(x) for x in row])
                results_file.flush()

                # Display some information about the benchmark.
                colorful.use_style('monokai')

                # First, we show the progress of the suite.
                n = len(inputs)
                percent = (i / n) * 100
                info = f'{colorful.bold}[{i:03}/{n:03}{colorful.reset}; '
                info += f'{percent:#.4}%] '

                # Next, we show the time taken to run this benchmark, the total
                # elapsed time, and the estimated time left.
                current_time = datetime.datetime.now()
                suite_duration = current_time - suite_start_time
                duration_per_iteration = suite_duration / i
                remaining_duration = (n - i) * duration_per_iteration

                def round_delta(d):
                    return datetime.timedelta(seconds=int(d.total_seconds()))

                info += f'{colorful.blue(round_delta(out.bench_duration))} / '
                info += f'{colorful.green(round_delta(suite_duration))} + '
                info += f'{colorful.magenta(round_delta(remaining_duration))}? '

                # Finally, we display a summary of the benchmark.
                summary = self.summary(out.input, out.output)
                info += f'{colorful.lightGray(summary)}'
                print(info)

        remote_thread.join()
        local_thread.join()


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


def _latency(s):
    return LatencyOutput(
        mean_ms=s.mean(),
        median_ms=s.median(),
        min_ms=s.min(),
        max_ms=s.max(),
        p90_ms=s.quantile(.90),
        p95_ms=s.quantile(.95),
        p99_ms=s.quantile(.99),
    )


def _throughput(s):
    return ThroughputOutput(
        mean=s.mean(),
        median=s.median(),
        min=s.min(),
        max=s.max(),
        p90=s.quantile(.90),
        p95=s.quantile(.95),
        p99=s.quantile(.99),
    )


def _wrangle_recorder_data(bench: BenchmarkDirectory,
                           filenames: Iterable[str],
                           drop_prefix: datetime.timedelta,
                           save_data: bool = True) -> pd.DataFrame:
    bench.log('Reading recorder data from the following CSVs:')
    for filename in filenames:
        bench.log(f'- {filename}')
    df = pd_util.read_csvs(filenames, parse_dates=['start', 'stop'])
    bench.log('Recorder data read.')

    bench.log('Setting aggregate recorder data index.')
    df = df.set_index('start')
    bench.log('Aggregate recorder data index set.')

    bench.log('Sorting aggregate recorder data on index.')
    df = df.sort_index(0)
    bench.log('Aggregate recorder data sorted on index.')

    if save_data:
        save_data_filename = bench.abspath('data.csv')
        bench.log(f'Saving aggregate recorder data to {save_data_filename}.')
        df.to_csv(save_data_filename)
        bench.log('Aggregate recorder data written.')

    # Throw away the original data. If `save_data` is true, it's stored in
    # `save_data_filename`.
    for filename in filenames:
        bench.log(f'Removing {filename}.')
        os.remove(filename)
    bench.log('Individual recorder data removed.')

    # We also compress the output data since it can get big.
    if save_data:
        bench.log('Compressing aggregate recorder data.')
        subprocess.call(['gzip', bench.abspath('data.csv')])
        bench.log('Aggregate recorder data compressed.')

    # Drop prefix of data.
    start_time = df.index[0]
    new_start_time = (start_time +
                      pd.DateOffset(seconds=drop_prefix.total_seconds()))
    bench.log('Dropping prefix of aggregate recorder data.')
    df = df[df.index >= new_start_time]
    bench.log('Prefix of aggregate recorder data dropped.')

    return df


# parse_recorder_data parses and summarizes data written by a
# frankenpaxos.BenchmarkUtil.Recorder.
#
# TODO(mwhittaker): Drop the first couple of seconds from the data since it
# takes a while for the JVM to fully ramp up.
def parse_recorder_data(bench: BenchmarkDirectory,
                        filenames: Iterable[str],
                        drop_prefix: datetime.timedelta,
                        save_data: bool = True) -> RecorderOutput:
    df = _wrangle_recorder_data(bench, filenames, drop_prefix, save_data)
    return RecorderOutput(
        latency=_latency(df['latency_nanos'] / 1e6),
        start_throughput_1s=_throughput(pd_util.throughput(df.index, 1000)),
    )


# parse_labeled_recorder_data parses and summarizes data written by a
# frankenpaxos.BenchmarkUtil.LabeledRecorder. Every label gets its own set of
# outputs.
def parse_labeled_recorder_data(bench: BenchmarkDirectory,
                                filenames: Iterable[str],
                                drop_prefix: datetime.timedelta,
                                save_data: bool = True) \
                                -> Dict[str, RecorderOutput]:
    df = _wrangle_recorder_data(bench, filenames, drop_prefix, save_data)

    # Record output for each label.
    outputs = dict()
    for label in df['label'].unique():
        bench.log(f'Computing on aggregate recorder data for {label}.')
        ldf = df[df['label'] == label]

        bench.log(f'- Computing latency.')
        latency = _latency(ldf['latency_nanos'] / 1e6)
        bench.log(f'- Latency computed.')

        bench.log(f'- Computing 1 second start throughput.')
        start_throughput_1s = \
            _throughput(pd_util.weighted_throughput(ldf['count'], 1000))
        bench.log(f'- 1 second start throughput computed.')

        outputs[label] = RecorderOutput(
            latency = latency,
            start_throughput_1s = start_throughput_1s,
        )
        bench.log(f'Aggregate recorder data for {label} computed.')

    return outputs
