package frankenpaxos.roundsystem

// Every Fast Paxos instance is associated with a set of integer-valued rounds.
// For every round r, we must asign r a unique leader, and we must classify r
// as a classic round or fast round. Moreover, every leader must be assigned an
// infinite number of rounds. A RoundSystem is such an assignment. Note that
// the name "round system" is a play on quorum systems [1].
//
// [1]: http://vukolic.com/QuorumsOrigin.pdf
sealed trait RoundType
case object ClassicRound extends RoundType
case object FastRound extends RoundType

trait RoundSystem {
  // Rounds are integer-valued. It is also common for rounds to be of the form
  // (a, i) where a is the address of a leader and i is an integer. Here, we
  // let rounds be integers to keep things simple.
  type Round = Int

  // A RoundSystem with n leaders assumes each leader is given a unique index
  // in the range [0, n).
  type LeaderIndex = Int

  // The number of leaders that this round system is designed for.
  def numLeaders(): Int

  // The leader of round `round`.
  def leader(round: Round): LeaderIndex

  // The type of round `round`.
  def roundType(round: Round): RoundType

  // The smallest classic round for `leaderIndex` greater than `round`. Every
  // leader is required to have an infinite number of classic rounds, so
  // nextClassicRound will always return a round. If round is less than 0, then
  // nextClassicRound returns the first classic round for `leaderIndex`.
  def nextClassicRound(leaderIndex: LeaderIndex, round: Round): Round

  // The smallest fast round for `leaderIndex` greater than `round`. Every
  // leader is required to have an infinite number of classic rounds, but it is
  // NOT guaranteed to have an infinite number of fast rounds. Thus,
  // nextFastRound only optionally returns a fast round. If round is less than
  // 0, then nextFastRound returns the first fast round for `leaderIndex`.
  def nextFastRound(leaderIndex: LeaderIndex, round: Round): Option[Round]
}

object RoundSystem {
  // A ClassicRoundRobin round system assigns classic rounds to leaders
  // round-robin. There are no fast rounds. Here's an example with n = 3:
  //
  //                       | Round | Leader | Round Type |
  //                       +-------+--------+------------+
  //                       | 0     | 0      | classic    |
  //                       | 1     | 1      | classic    |
  //                       | 2     | 2      | classic    |
  //                       | 3     | 0      | classic    |
  //                       | 4     | 1      | classic    |
  //                       | 5     | 2      | classic    |
  //                       | 6     | 0      | classic    |
  class ClassicRoundRobin(private val n: Int) extends RoundSystem {
    override def toString(): String = s"ClassicRoundRobin($n)"
    override def numLeaders(): Int = n
    override def leader(round: Round): LeaderIndex = round % n
    override def roundType(round: Round): RoundType = ClassicRound

    override def nextClassicRound(
        leaderIndex: LeaderIndex,
        round: Round
    ): Round = {
      if (round < 0) {
        leaderIndex
      } else {
        val smallestMultipleOfN = n * (round / n)
        val offset = leaderIndex % n
        if (smallestMultipleOfN + offset > round) {
          smallestMultipleOfN + offset
        } else {
          smallestMultipleOfN + n + offset
        }
      }
    }

    override def nextFastRound(
        leaderIndex: LeaderIndex,
        round: Round
    ): Option[Round] = None
  }

  // A RoundZeroFast round system assigns round 0 to leader 0 and then assigns
  // rounds round-robin. Round 0 is fast, and all other rounds are classic.
  // This round system is used in BPaxos (and implicitly in EPaxos). Here's an
  // example with n = 3:
  //
  //                       | Round | Leader | Round Type |
  //                       +-------+--------+------------+
  //                       | 0     | 0      | fast       |
  //                       | 1     | 0      | classic    |
  //                       | 2     | 1      | classic    |
  //                       | 3     | 2      | classic    |
  //                       | 4     | 0      | classic    |
  //                       | 5     | 1      | classic    |
  //                       | 6     | 2      | classic    |
  class RoundZeroFast(private val n: Int) extends RoundSystem {
    override def toString(): String = s"RoundZeroFast($n)"
    override def numLeaders(): Int = n

    override def leader(round: Round): LeaderIndex = {
      if (round == 0) {
        0
      } else {
        (round - 1) % n
      }
    }

    override def roundType(round: Round): RoundType = {
      if (round == 0) FastRound else ClassicRound
    }

    override def nextClassicRound(
        leaderIndex: LeaderIndex,
        round: Round
    ): Round = {
      1 + new ClassicRoundRobin(n).nextClassicRound(leaderIndex, round - 1)
    }

    override def nextFastRound(
        leaderIndex: LeaderIndex,
        round: Round
    ): Option[Round] = {
      if (leaderIndex == 0 && round < 0) Some(0) else None
    }
  }

  // A MixedRoundRobin round system assigns pairs of contiguous fast and
  // classic rounds round-robin. Here's an example with n = 3:
  //
  //                       | Round | Leader | Round Type |
  //                       +-------+--------+------------+
  //                       | 0     | 0      | fast       |
  //                       | 1     | 0      | classic    |
  //                       | 2     | 1      | fast       |
  //                       | 3     | 1      | classic    |
  //                       | 4     | 2      | fast       |
  //                       | 5     | 2      | classic    |
  //                       | 6     | 0      | fast       |
  //                       | 7     | 0      | classic    |
  //                       | 8     | 1      | fast       |
  //                       | 9     | 1      | classic    |
  class MixedRoundRobin(private val n: Int) extends RoundSystem {
    override def toString(): String = s"MixedRoundRobin($n)"
    override def numLeaders(): Int = n
    override def leader(round: Round): LeaderIndex = (round / 2) % n

    override def roundType(round: Round): RoundType = {
      if (round % 2 == 0) FastRound else ClassicRound
    }

    override def nextClassicRound(
        leaderIndex: LeaderIndex,
        round: Round
    ): Round = {
      // If round is a fast round of leaderIndex, then the next classic round
      // is the next round. Otherwise, the next classic round is the round
      // after the next fast round.
      if (round / 2 % n == leaderIndex && round % 2 == 0) {
        round + 1
      } else {
        nextFastRound(leaderIndex, round).get + 1
      }
    }

    override def nextFastRound(
        leaderIndex: LeaderIndex,
        round: Round
    ): Option[Round] = {
      if (round < 0) {
        Some(leaderIndex * 2)
      } else {
        Some(
          new ClassicRoundRobin(n).nextClassicRound(leaderIndex, round / 2) * 2
        )
      }
    }
  }

  // A RenamedRoundSsytem allows you to adapt an existing round system by
  // renaming the leaders. For example, imagine we have the following round
  // system:
  //
  //                      round: 0 1 2 3 4 5 6 7 8 9 ...
  //                       type: F C C C C C C C C C ...
  //                     leader: 0 1 2 0 1 2 0 1 2 0 ...
  //
  // If we rename leader 1 to leader 2, and leader 2 to leader 1, we get the
  // following renaming:
  //
  //                      round: 0 1 2 3 4 5 6 7 8 9 ...
  //       ORIGINAL        type: F C C C C C C C C C ...
  //                     leader: 0 1 2 0 1 2 0 1 2 0 ...
  //
  //                             | | | | | | | | | |
  //                             v v v v v v v v v v
  //
  //                      round: 0 1 2 3 4 5 6 7 8 9 ...
  //       RENAMED         type: F C C C C C C C C C ...
  //                     leader: 0 2 1 0 2 1 0 2 1 0 ...
  //
  // We call such a mapping a renaming. The inverse mapping is an unrenaming.
  // We can use the rename and unrename transformations to adapt the existing
  // round system.
  class RenamedRoundSystem(
      // roundSystem is the round system being adapted.
      private val roundSystem: RoundSystem,
      // renaming is a permutation of leaders.
      private val renaming: Map[Int, Int]
  ) extends RoundSystem {
    // unrenaming is the inverse permutation of renaming.
    private val unrenaming: Map[Int, Int] = renaming.map({
      case (k, v) => (v, k)
    })

    override def toString(): String = s"Renamed($roundSystem, $renaming)"

    override def numLeaders(): Int = roundSystem.numLeaders()

    override def leader(round: Round): LeaderIndex =
      renaming(roundSystem.leader(round))

    override def roundType(round: Round): RoundType =
      roundSystem.roundType(round)

    override def nextClassicRound(
        leaderIndex: LeaderIndex,
        round: Round
    ): Round = {
      roundSystem.nextClassicRound(unrenaming(leaderIndex), round)
    }

    override def nextFastRound(
        leaderIndex: LeaderIndex,
        round: Round
    ): Option[Round] = {
      roundSystem.nextFastRound(unrenaming(leaderIndex), round)
    }
  }

  // A RotatedRoundSystem is a renamed round system in which the identity of
  // the leaders is rotated. For example, given 5 leaders and a rotation of 2,
  // we get the following mapping:
  //
  //                              0 1 2 3 4
  //                              | | | | |
  //                              v v v v v
  //                              2 3 4 0 1
  class RotatedRoundSystem(
      private val roundSystem: RoundSystem,
      private var rotation: Int
  ) extends RoundSystem {
    private def rotate(leaderIndex: LeaderIndex): LeaderIndex = {
      val x = (leaderIndex + rotation) % roundSystem.numLeaders()
      if (x < 0) {
        x + roundSystem.numLeaders()
      } else {
        x
      }
    }

    private val renamedRoundSystem = new RenamedRoundSystem(
      roundSystem,
      (0 until roundSystem.numLeaders).map(i => (i, rotate(i))).toMap
    )

    def numLeaders(): Int = renamedRoundSystem.numLeaders()
    def leader(round: Round): LeaderIndex = renamedRoundSystem.leader(round)
    def roundType(round: Round): RoundType = renamedRoundSystem.roundType(round)
    def nextClassicRound(leaderIndex: LeaderIndex, round: Round): Round =
      renamedRoundSystem.nextClassicRound(leaderIndex, round)
    def nextFastRound(leaderIndex: LeaderIndex, round: Round): Option[Round] =
      renamedRoundSystem.nextFastRound(leaderIndex, round)
  }

  // A RotatedClassicRoundRobin round system is a rotated ClassicRoundRobin
  // round system. Here's an example with n = 3 and firstLeader = 1:
  //
  //                       | Round | Leader | Round Type |
  //                       +-------+--------+------------+
  //                       | 0     | 1      | classic    |
  //                       | 1     | 2      | classic    |
  //                       | 2     | 0      | classic    |
  //                       | 3     | 1      | classic    |
  //                       | 4     | 2      | classic    |
  //                       | 5     | 0      | classic    |
  //                       | 6     | 1      | classic    |
  //
  // Here's an example with n = 3 and firstLeader = 2:
  //
  //                       | Round | Leader | Round Type |
  //                       +-------+--------+------------+
  //                       | 0     | 2      | classic    |
  //                       | 1     | 0      | classic    |
  //                       | 2     | 1      | classic    |
  //                       | 3     | 2      | classic    |
  //                       | 4     | 0      | classic    |
  //                       | 5     | 1      | classic    |
  //                       | 6     | 2      | classic    |
  class RotatedClassicRoundRobin(
      private val n: Int,
      private val firstLeader: Int
  ) extends RotatedRoundSystem(new ClassicRoundRobin(n), firstLeader) {
    override def toString(): String =
      s"RotatedClassicRoundRobin($n, $firstLeader)"
  }

  // A RotatedRoundZeroFast round system is a rotated RoundZeroFast round
  // system. Here's an example with n = 3 and firstLeader = 1:
  //
  //                       | Round | Leader | Round Type |
  //                       +-------+--------+------------+
  //                       | 0     | 1      | fast       |
  //                       | 1     | 1      | classic    |
  //                       | 2     | 2      | classic    |
  //                       | 3     | 0      | classic    |
  //                       | 4     | 1      | classic    |
  //                       | 5     | 2      | classic    |
  //                       | 6     | 0      | classic    |
  //
  // Here's an example with n = 3 and firstLeader = 2:
  //
  //                       | Round | Leader | Round Type |
  //                       +-------+--------+------------+
  //                       | 0     | 2      | fast       |
  //                       | 1     | 2      | classic    |
  //                       | 2     | 0      | classic    |
  //                       | 3     | 1      | classic    |
  //                       | 4     | 2      | classic    |
  //                       | 5     | 0      | classic    |
  //                       | 6     | 1      | classic    |
  class RotatedRoundZeroFast(
      private val n: Int,
      private val firstLeader: Int
  ) extends RotatedRoundSystem(new RoundZeroFast(n), firstLeader) {
    override def toString(): String =
      s"RotatedRoundZeroFast($n, $firstLeader)"
  }
}
