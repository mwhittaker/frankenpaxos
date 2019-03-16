from typing import Dict, IO
import datetime
import json
import os
import random
import string

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

    def __str__(self) -> str:
        return f'BenchmarkDirectory({self.path})'

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

def _random_string(n: int) -> str:
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(n))

def _now_string():
    return str(datetime.datetime.now()).replace(' ', '_')
