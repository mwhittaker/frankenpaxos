from . import pd_util
from typing import Tuple
import argparse
import os
import pandas as pd

def get_benchmark_parser() -> argparse.ArgumentParser:
    """
    get_benchmark_parser returns an argument parser with the flags most
    commonly used by benchmark scripts.
    """
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
