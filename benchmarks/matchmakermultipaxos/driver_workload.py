from .. import proto_util
from typing import List, NamedTuple, Union


class DoNothing(NamedTuple):
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'DoNothing'

    def to_proto(self) -> proto_util.Message:
        return {
            'do_nothing': dict()
        }


class RepeatedLeaderReconfiguration(NamedTuple):
    acceptors: List[int]
    delay_ms: int
    period_ms: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'RepeatedLeaderReconfiguration'

    def to_proto(self) -> proto_util.Message:
        return {
            'repeated_leader_reconfiguration': {
                'acceptor': self.acceptors,
                'delay_ms': self.delay_ms,
                'period_ms': self.period_ms,
            }
        }


class DoubleLeaderReconfiguration(NamedTuple):
    first_reconfiguration_delay_ms: int
    first_reconfiguration: List[int]
    acceptor_failure_delay_ms: int
    acceptor_failure: int
    second_reconfiguration_delay_ms: int
    second_reconfiguration: List[int]
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'DoubleLeaderReconfiguration'

    def to_proto(self) -> proto_util.Message:
        return {
            'double_leader_reconfiguration': {
                'first_reconfiguration_delay_ms':
                    self.first_reconfiguration_delay_ms,
                'first_reconfiguration':
                    self.first_reconfiguration,
                'acceptor_failure_delay_ms':
                    self.acceptor_failure_delay_ms,
                'acceptor_failure':
                    self.acceptor_failure,
                'second_reconfiguration_delay_ms':
                    self.second_reconfiguration_delay_ms,
                'second_reconfiguration':
                    self.second_reconfiguration,
            }
        }


DriverWorkload = Union[DoNothing, RepeatedLeaderReconfiguration,
                       DoubleLeaderReconfiguration]
