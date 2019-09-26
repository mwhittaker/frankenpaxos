from . import proto_util
from typing import NamedTuple, Union


class StringWorkload(NamedTuple):
    size_mean: int
    size_std: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'StringWorkload'

    def to_proto(self) -> proto_util.Message:
        return {
            'string_workload': {
                'size_mean': self.size_mean,
                'size_std': self.size_std,
            }
        }


class UniformSingleKeyWorkload(NamedTuple):
    num_keys: int
    size_mean: int
    size_std: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'UniformSingleKeyWorkload'

    def to_proto(self) -> proto_util.Message:
        return {
            'uniform_single_key_workload': {
                'num_keys': self.num_keys,
                'size_mean': self.size_mean,
                'size_std': self.size_std,
            }
        }

class BernoulliSingleKeyWorkload(NamedTuple):
    conflict_rate: float
    size_mean: int
    size_std: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'BernoulliSingleKeyWorkload'

    def to_proto(self) -> proto_util.Message:
        return {
            'bernoulli_single_key_workload': {
                'conflict_rate': self.conflict_rate,
                'size_mean': self.size_mean,
                'size_std': self.size_std,
            }
        }

class BinomialSingleKeyWorkload(NamedTuple):
    conflict_rate: float
    size_mean: int
    size_std: int
    n: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'BinomialSingleKeyWorkload'

    def to_proto(self) -> proto_util.Message:
        return {
            'binomial_single_key_workload': {
                'conflict_rate': self.conflict_rate,
                'size_mean': self.size_mean,
                'size_std': self.size_std,
                'n': self.n,
            }
        }

Workload = Union[
    StringWorkload,
    UniformSingleKeyWorkload,
    BernoulliSingleKeyWorkload,
    BinomialSingleKeyWorkload,
]
