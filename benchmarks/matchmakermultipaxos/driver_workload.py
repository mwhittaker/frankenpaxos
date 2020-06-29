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
    reconfiguration_warmup_delay_ms: int
    reconfiguration_warmup_period_ms: int
    reconfiguration_warmup_num: int
    reconfiguration_delay_ms: int
    reconfiguration_period_ms: int
    reconfiguration_num: int
    failure_delay_ms: int
    recover_delay_ms: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'LeaderReconfiguration'

    def to_proto(self) -> proto_util.Message:
        return {
            'leader_reconfiguration': {
                'reconfiguration_warmup_delay_ms':
                    self.reconfiguration_warmup_delay_ms,
                'reconfiguration_warmup_period_ms':
                    self.reconfiguration_warmup_period_ms,
                'reconfiguration_warmup_num':
                    self.reconfiguration_warmup_num,
                'reconfiguration_delay_ms':
                    self.reconfiguration_delay_ms,
                'reconfiguration_period_ms':
                    self.reconfiguration_period_ms,
                'reconfiguration_num':
                    self.reconfiguration_num,
                'failure_delay_ms': self.failure_delay_ms,
                'recover_delay_ms': self.recover_delay_ms,
            }
        }


class MatchmakerReconfiguration(NamedTuple):
    reconfiguration_warmup_delay_ms: int
    reconfiguration_warmup_period_ms: int
    reconfiguration_warmup_num: int
    matchmaker_reconfiguration_delay_ms: int
    matchmaker_reconfiguration_period_ms: int
    matchmaker_reconfiguration_num: int
    failure_delay_ms: int
    recover_delay_ms: int
    reconfigure_delay_ms: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'MatchmakerReconfiguration'

    def to_proto(self) -> proto_util.Message:
        return {
            'matchmaker_reconfiguration': {
                'reconfiguration_warmup_delay_ms':
                    self.reconfiguration_warmup_delay_ms,
                'reconfiguration_warmup_period_ms':
                    self.reconfiguration_warmup_period_ms,
                'reconfiguration_warmup_num':
                    self.reconfiguration_warmup_num,
                'matchmaker_reconfiguration_delay_ms':
                    self.matchmaker_reconfiguration_delay_ms,
                'matchmaker_reconfiguration_period_ms':
                    self.matchmaker_reconfiguration_period_ms,
                'matchmaker_reconfiguration_num':
                    self.matchmaker_reconfiguration_num,
                'failure_delay_ms': self.failure_delay_ms,
                'recover_delay_ms': self.recover_delay_ms,
                'reconfigure_delay_ms': self.reconfigure_delay_ms,
            }
        }


class LeaderFailure(NamedTuple):
    leader_change_warmup_delay_ms: int
    leader_change_warmup_period_ms: int
    leader_change_warmup_num: int
    failure_delay_ms: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'LeaderFailure'

    def to_proto(self) -> proto_util.Message:
        return {
            'leader_failure': {
                'leader_change_warmup_delay_ms':
                    self.leader_change_warmup_delay_ms,
                'leader_change_warmup_period_ms':
                    self.leader_change_warmup_period_ms,
                'leader_change_warmup_num':
                    self.leader_change_warmup_num,
                'failure_delay_ms':
                    self.failure_delay_ms,
            }
        }


class Chaos(NamedTuple):
    leader_change_warmup_delay_ms: int
    leader_change_warmup_period_ms: int
    leader_change_warmup_num: int
    reconfiguration_warmup_delay_ms: int
    reconfiguration_warmup_period_ms: int
    reconfiguration_warmup_num: int
    matchmaker_reconfiguration_warmup_delay_ms: int
    matchmaker_reconfiguration_warmup_period_ms: int
    matchmaker_reconfiguration_warmup_num: int
    leader_failure_delay_ms: int
    acceptor_failure_delay_ms: int
    matchmaker_failure_delay_ms: int
    acceptor_recover_delay_ms: int
    matchmaker_recover_delay_ms: int
    # We put the name here so that it appears in benchmark outputs.
    name: str = 'Chaos'

    def to_proto(self) -> proto_util.Message:
        return {
            'leader_failure': {
                'leader_change_warmup_delay_ms':
                    self.leader_change_warmup_delay_ms,
                'leader_change_warmup_period_ms':
                    self.leader_change_warmup_period_ms,
                'leader_change_warmup_num':
                    self.leader_change_warmup_num,
                'reconfiguration_warmup_delay_ms':
                    self.reconfiguration_warmup_delay_ms,
                'reconfiguration_warmup_period_ms':
                    self.reconfiguration_warmup_period_ms,
                'reconfiguration_warmup_num':
                    self.reconfiguration_warmup_num,
                'matchmaker_reconfiguration_warmup_delay_ms':
                    self.matchmaker_reconfiguration_warmup_delay_ms,
                'matchmaker_reconfiguration_warmup_period_ms':
                    self.matchmaker_reconfiguration_warmup_period_ms,
                'matchmaker_reconfiguration_warmup_num':
                    self.matchmaker_reconfiguration_warmup_num,
                'leader_failure_delay_ms':
                    self.leader_failure_delay_ms,
                'acceptor_failure_delay_ms':
                    self.acceptor_failure_delay_ms,
                'matchmaker_failure_delay_ms':
                    self.matchmaker_failure_delay_ms,
                'acceptor_recover_delay_ms':
                    self.acceptor_recover_delay_ms,
                'matchmaker_recover_delay_ms':
                    self.matchmaker_recover_delay_ms,
            }
        }


DriverWorkload = Union[DoNothing, RepeatedLeaderReconfiguration,
                       LeaderReconfiguration, MatchmakerReconfiguration,
                       LeaderFailure, Chaos]
