from .util import Reaped
from contextlib import ExitStack
from typing import Any, Dict, IO, List
import datetime
import json
import os
import random
import string
import subprocess

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
        self.process_stack = ExitStack()

        # A mapping from pid to command label.
        self.pids: Dict[int, str] = dict()

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

    def popen(self,
              f: Any,
              label: str,
              cmd: List[str],
              out = None,
              err = None,
              **kwargs) -> subprocess.Popen:
        """Runs a command within this directory.

        `popen` runs a command, recording the command, its stdout, and its
        stderr within the benchmark directory. `f` is a function like
        subprocess.Popen. `label` is an arbitrary name for the command. `cmd`
        and `kwargs` are passed to `f`.
        """
        self.write_string(f'{label}_cmd.txt', ' '.join(cmd))
        out = out or self.create_file(f'{label}_out.txt')
        err = err or self.create_file(f'{label}_err.txt')
        proc = f(cmd, stdout=out, stderr=err, **kwargs)
        self.process_stack.enter_context(Reaped(proc))
        self.pids[proc.pid] = label
        return proc

def _random_string(n: int) -> str:
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(n))

def _now_string():
    return str(datetime.datetime.now()).replace(' ', '_')
