from .. import proto_util
from typing import NamedTuple, Union


class DoNothing(NamedTuple):
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'DoNothing'

    def to_proto(self) -> proto_util.Message:
        return {
            'do_nothing': dict()
        }


class DoubleLeaderReconfiguration(NamedTuple):
    first_reconfiguration_delay_ms: int
    first_reconfiguration: int
    acceptor_failure_delay_ms: int
    acceptor_failure: int
    second_reconfiguration_delay_ms: int
    second_reconfiguration: int
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


DriverWorkload = Union[DoNothing, DoubleLeaderReconfiguration]
