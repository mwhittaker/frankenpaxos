from typing import Any, Dict, IO, List, Union
import contextlib
import datetime
import json
import os
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
    Imagine you have the following python code in a file called sleep.py. The
    code creates a subprocess and waits for it to terminate.

      p = subprocess.Popen(['sleep', '100'])
      p.wait()

    If you run `python sleep.py` and then kill the process before it terminates,
    sometimes the subprocess lives on. The _Reaped context manager ensures that
    the process is killed, even if an exception is thrown.
    """
    def __init__(self, p: subprocess.Popen) -> None:
        self.p = p

    def __enter__(self) -> subprocess.Popen:
        return self.p

    def __exit__(self, cls, exn, traceback) -> None:
        self.p.terminate()


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
        self.write_string('end_time.txt', str(datetime.datetime.now()))

    def abspath(self, filename: str) -> str:
        return os.path.join(self.path, filename)

    def create_file(self, filename: str) -> IO:
        return open(self.abspath(filename), 'w')

    def write_string(self, filename: str, s: str) -> str:
        with self.create_file(filename) as f:
            f.write(s + '\n')
        return self.abspath(filename)

    def write_dict(self, filename: str, d: Dict) -> str:
        self.write_string(filename, json.dumps(d, indent=4))
        return self.abspath(filename)

    def benchmark_directory(self, name: str = None) -> 'BenchmarkDirectory':
        benchmark_dir_id = self.benchmark_dir_id
        self.benchmark_dir_id += 1
        name_suffix = ("_" + name) if name else ""
        path = os.path.join(self.path,
                            "{:03}{}".format(benchmark_dir_id, name_suffix))
        return BenchmarkDirectory(path)


class BenchmarkDirectory(object):
    def __init__(self, path: str) -> None:
        assert not os.path.exists(path)
        self.path = os.path.abspath(path)
        os.makedirs(self.path)

        # We want to ensure that all processes run within a benchmark are
        # terminated if the benchmark is killed. Thus, we put all processes in
        # this stack.
        self.process_stack = contextlib.ExitStack()

        # A mapping from pid to command label.
        self.pids: Dict[int, str] = dict()

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
        self.write_dict('pids.json', self.pids)
        self.write_string('end_time.txt', str(datetime.datetime.now()))

    def abspath(self, filename: str) -> str:
        return os.path.join(self.path, filename)

    def create_file(self, filename: str) -> IO:
        return open(self.abspath(filename), 'w')

    def write_string(self, filename: str, s: str) -> str:
        with self.create_file(filename) as f:
            f.write(s + '\n')
        return self.abspath(filename)

    def write_dict(self, filename: str, d: Dict) -> str:
        self.write_string(filename, json.dumps(d, indent=4))
        return self.abspath(filename)

    def log(self, s: str) -> None:
        self.logfile.write(f'[{_pretty_now_string()}] {s}\n')
        self.logfile.flush()

    def popen(self,
              label: str,
              cmd: List[str],
              out = None,
              err = None,
              f = None,
              profile: bool = False,
              profile_frequency: int = 1000,
              **kwargs) -> Union[subprocess.Popen, '_ProfiledPopen']:
        """Runs a command within this directory.

        `popen` runs a command, recording the command, its stdout, and its
        stderr within the benchmark directory. For example,

            bench.popen('ls', ['ls', '-l'])

        runs `ls -l`. The string `ls -l` is written to `ls_cmd.txt`. The stdout
        and stderr of `ls -l` are written to `ls_out.txt` and `ls_err.txt`
        respectively.

        Some notes on arguments:

          - `f` is a function that behaves like subprocess.Popen. By default it
            is subprocess.Popen.
          - If `profile` is true, then `cmd` is assumed to be a java process
            and is profiled with perf. The resulting stack traces are written
            and compressed to to `label_stacks.txt.gz`.
        """
        self.write_string(f'{label}_cmd.txt', ' '.join(cmd))
        out = out or self.create_file(f'{label}_out.txt')
        err = err or self.create_file(f'{label}_err.txt')
        f = f or subprocess.Popen
        proc = f(cmd, stdout=out, stderr=err, **kwargs)

        if profile:
            proc = _ProfiledPopen(
                bench=self,
                label=label,
                proc=proc,
                f=f,
                profile_frequency=profile_frequency)

            # Perf records data in files of the form perf-<pid>.data and
            # perf-<pid>.map. It's hard to know which pid corresponds to which
            # executable, so we record the pids in self.pids and write them out
            # at the end of the benchmark.
            self.pids[proc.pid] = label

        # Make sure the process is eventually killed.
        self.process_stack.enter_context(_Reaped(proc))
        return proc


class _ProfiledPopen(object):
    """A _ProfiledPopen looks like a subprocess.Popen, but isn't."""

    def __init__(self,
                 bench: BenchmarkDirectory,
                 label: str,
                 proc: subprocess.Popen,
                 f,
                 profile_frequency: int) -> None:
        self.bench = bench
        self.label = label
        self.proc = proc
        self.f = f
        self.terminated = False

        self.perf_record_proc = self.bench.popen(
            label=f'{label}_perf_record',
            cmd=[
                'perf',
                'record',
                '-F', str(profile_frequency),
                '-o', f'/tmp/perf-{self.proc.pid}.data',
                '-p', str(self.proc.pid),
                '--timestamp',
                '-g',
                '--', 'sleep', '1000000000000000000000'
            ],
            f=f,
        )

    @property
    def args(self):
        return self.proc.args

    @property
    def stdin(self):
        return self.proc.stdin

    @property
    def stdout(self):
        return self.proc.stdout

    @property
    def pid(self):
        return self.proc.pid

    @property
    def returncode(self):
        return self.proc.returncode

    def poll(self, *args, **kwargs):
        return self.proc.poll(*args, **kwargs)

    def wait(self, *args, **kwargs):
        return self.proc.wait(*args, **kwargs)

    def communicate(self, *args, **kwargs):
        return self.proc.communicate(*args, **kwargs)

    def send_signal(self, *args, **kwargs):
        return self.proc.send_signal(*args, **kwargs)

    def terminate(self, *args, **kwargs):
        if self.terminated:
            return

        # Stop perf recording.
        self.perf_record_proc.terminate()

        # Generate the java map while the process is still running.
        self.bench.popen(
            label=f'{self.label}_perf_map',
            cmd = ['create-java-perf-map.sh', str(self.proc.pid)],
            f=self.f,
        ).wait()

        # Extract the stack traces.
        self.bench.popen(
            label=f'{self.label}_perf_script',
            cmd=['perf', 'script', '-i', f'/tmp/perf-{self.proc.pid}.data'],
            out=self.bench.create_file(f'{self.label}_stacks.txt'),
            f=self.f,
        ).wait()
        subprocess.call(['gzip',
                         self.bench.abspath(f'{self.label}_stacks.txt')])

        # And finally, kill the process.
        self.proc.terminate(*args, **kwargs)
        self.terminated = True

    def kill(self, *args, **kwargs):
        return self.proc.kill(*args, **kwargs)
