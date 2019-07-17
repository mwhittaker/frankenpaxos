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


Workload = Union[StringWorkload, UniformSingleKeyWorkload]
