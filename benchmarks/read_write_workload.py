from . import proto_util
from . import workload
from typing import NamedTuple, Union


class UniformReadWriteWorkload(NamedTuple):
    num_keys: int
    read_fraction: float
    write_size_mean: int
    write_size_std: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'UniformReadWriteWorkload'

    def to_proto(self) -> proto_util.Message:
        return {
            'uniform_read_write_workload': {
                'num_keys': self.num_keys,
                'read_fraction': self.read_fraction,
                'write_size_mean': self.write_size_mean,
                'write_size_std': self.write_size_std,
            }
        }


class UniformMultiKeyReadWriteWorkload(NamedTuple):
    num_keys: int
    num_operations: int
    read_fraction: float
    write_size_mean: int
    write_size_std: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'UniformMultiKeyReadWriteWorkload'

    def to_proto(self) -> proto_util.Message:
        return {
            'uniform_multi_key_read_write_workload': {
                'num_keys': self.num_keys,
                'num_operations': self.num_operations,
                'read_fraction': self.read_fraction,
                'write_size_mean': self.write_size_mean,
                'write_size_std': self.write_size_std,
            }
        }


class WriteOnlyStringWorkload(NamedTuple):
    workload: workload.StringWorkload
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'WriteOnlyStringWorkload'

    def to_proto(self) -> proto_util.Message:
        return {
            'write_only_string_workload': {
                'workload': self.workload,
            }
        }


class WriteOnlyUniformSingleKeyWorkload(NamedTuple):
    workload: workload.UniformSingleKeyWorkload
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'WriteOnlyUniformSingleKeyWorkload'

    def to_proto(self) -> proto_util.Message:
        return {
            'write_only_uniform_single_key_workload': {
                'workload': self.workload,
            }
        }

class WriteOnlyBernoulliSingleKeyWorkload(NamedTuple):
    workload: workload.BernoulliSingleKeyWorkload
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'WriteOnlyBernoulliSingleKeyWorkload'

    def to_proto(self) -> proto_util.Message:
        return {
            'write_only_uniform_single_key_workload': {
                'workload': self.workload,
            }
        }


ReadWriteWorkload = Union[UniformReadWriteWorkload,
                          UniformMultiKeyReadWriteWorkload,
                          WriteOnlyStringWorkload,
                          WriteOnlyUniformSingleKeyWorkload,
                          WriteOnlyBernoulliSingleKeyWorkload]
