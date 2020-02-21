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


class LeaderReconfiguration(NamedTuple):
    warmup_delay_ms: int
    warmup_period_ms: int
    warmup_num: int
    delay_ms: int
    period_ms: int
    num: int
    failure_delay_ms: int
    recover_delay_ms: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'LeaderReconfiguration'

    def to_proto(self) -> proto_util.Message:
        return {
            'leader_reconfiguration': {
                'warmup_delay_ms': self.warmup_delay_ms,
                'warmup_period_ms': self.warmup_period_ms,
                'warmup_num': self.warmup_num,
                'delay_ms': self.delay_ms,
                'period_ms': self.period_ms,
                'num': self.num,
                'failure_delay_ms': self.failure_delay_ms,
                'recover_delay_ms': self.recover_delay_ms,
            }
        }


DriverWorkload = Union[DoNothing, RepeatedLeaderReconfiguration,
                       LeaderReconfiguration]
