import datetime
import json
import os
import random
import string

def random_string(n):
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(n))

def now_string():
    return str(datetime.datetime.now()).replace(' ', '_')

class SuiteDirectory(object):
    def __init__(self, path, name=None):
        assert os.path.exists(path)

        self.benchmark_dir_id = 1

        name_suffix = ("_" + name) if name else ""
        self.path = os.path.join(
            os.path.abspath(path),
            now_string() + '_' + random_string(10) + name_suffix)
        assert not os.path.exists(self.path)
        os.makedirs(self.path)

    def create_file(self, filename):
        path = os.path.join(self.path, filename)
        return open(path, 'w')

    def write_string(self, filename, s):
        with self.create_file(filename) as f:
            f.write(s + '\n')

    def write_dict(self, filename, d):
        self.write_string(filename, json.dumps(d, indent=4))

    def new_benchmark_directory(self, name=None):
        benchmark_dir_id = self.benchmark_dir_id
        self.benchmark_dir_id += 1
        name_suffix = ("_" + name) if name else ""
        path = os.path.join(self.path,
                            "{:03}{}".format(benchmark_dir_id, name_suffix))
        return BenchmarkDirectory(path)

class BenchmarkDirectory(object):
    def __init__(self, path):
        assert not os.path.exists(path)
        self.path = os.path.abspath(path)
        os.makedirs(self.path)

    def create_file(self, filename):
        path = os.path.join(self.path, filename)
        return open(path, 'w')

    def write_string(self, filename, s):
        with self.create_file(filename) as f:
            f.write(s + '\n')

    def write_dict(self, filename, d):
        self.write_string(filename, json.dumps(d, indent=4))

    def __enter__(self):
        self.write_string('start_time.txt', str(datetime.datetime.now()))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.write_string('end_time.txt', str(datetime.datetime.now()))
