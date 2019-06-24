from .. import benchmark
from typing import Any, Collection, Dict, NamedTuple
import json
import time

class Input(NamedTuple):
    num_iterations: int
    num_adds_per_iteration: int

class Output(NamedTuple):
    latency_seconds: float

class ExampleSuite(benchmark.Suite[Input, Output]):
    def args(self) -> Dict[Any, Any]:
        return {}

    def inputs(self) -> Collection[Input]:
        return [
            Input(
                num_iterations=num_iterations,
                num_adds_per_iteration=num_adds_per_iteration,
            )
            for num_iterations in [10000, 20000, 30000]
            for num_adds_per_iteration in [10000, 20000]
        ]

    def summary(self, input: Input, output: Output) -> str:
        return str({
            'num_iterations': input.num_iterations,
            'latency_seconds': f'{output.latency_seconds:.2}'
        })

    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        start = time.time()
        for _ in range(input.num_iterations):
            x = 0
            for _ in range(input.num_adds_per_iteration):
                x += 1
        stop = time.time()
        return Output(latency_seconds=stop - start)

def main():
    suite = ExampleSuite()
    with benchmark.SuiteDirectory('/tmp', 'example') as suite_dir:
        suite.run_suite(suite_dir)

if __name__ == '__main__':
    main()
