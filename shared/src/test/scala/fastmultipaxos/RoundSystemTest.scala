package frankenpaxos.fastmultipaxos

import org.scalatest._

class RoundSystemSpec extends FlatSpec {
  "ClassicRoundRobin" should "implement numLeaders correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)
    assertResult(3)(rs.numLeaders)
  }

  it should "implement leader correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)
    assertResult(0)(rs.leader(0))
    assertResult(1)(rs.leader(1))
    assertResult(2)(rs.leader(2))
    assertResult(0)(rs.leader(3))
    assertResult(1)(rs.leader(4))
    assertResult(2)(rs.leader(5))
    assertResult(0)(rs.leader(6))
    assertResult(1)(rs.leader(7))
    assertResult(2)(rs.leader(8))
  }

  it should "implement roundType correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)
    for (i <- 0 to 10) {
      assertResult(ClassicRound)(rs.roundType(i))
    }
  }

  it should "implement nextClassicRound correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)

    assertResult(0)(rs.nextClassicRound(leaderIndex = 0, round = -1))
    assertResult(3)(rs.nextClassicRound(leaderIndex = 0, round = 0))
    assertResult(3)(rs.nextClassicRound(leaderIndex = 0, round = 1))
    assertResult(3)(rs.nextClassicRound(leaderIndex = 0, round = 2))
    assertResult(6)(rs.nextClassicRound(leaderIndex = 0, round = 3))
    assertResult(6)(rs.nextClassicRound(leaderIndex = 0, round = 4))
    assertResult(6)(rs.nextClassicRound(leaderIndex = 0, round = 5))
    assertResult(9)(rs.nextClassicRound(leaderIndex = 0, round = 6))

    assertResult(1)(rs.nextClassicRound(leaderIndex = 1, round = -1))
    assertResult(1)(rs.nextClassicRound(leaderIndex = 1, round = 0))
    assertResult(4)(rs.nextClassicRound(leaderIndex = 1, round = 1))
    assertResult(4)(rs.nextClassicRound(leaderIndex = 1, round = 2))
    assertResult(4)(rs.nextClassicRound(leaderIndex = 1, round = 3))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 1, round = 4))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 1, round = 5))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 1, round = 6))

    assertResult(2)(rs.nextClassicRound(leaderIndex = 2, round = -1))
    assertResult(2)(rs.nextClassicRound(leaderIndex = 2, round = 0))
    assertResult(2)(rs.nextClassicRound(leaderIndex = 2, round = 1))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 2))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 3))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 4))
    assertResult(8)(rs.nextClassicRound(leaderIndex = 2, round = 5))
    assertResult(8)(rs.nextClassicRound(leaderIndex = 2, round = 6))
  }

  it should "implement nextFastRound correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)
    for (leader <- 0 to 3) {
      for (round <- -1 to 10) {
        assertResult(None)(rs.nextFastRound(leader, round))
      }
    }
  }

  "RoundZeroFast" should "implement numLeaders correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    assertResult(3)(rs.numLeaders)
  }

  it should "implement leader correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    assertResult(0)(rs.leader(0))
    assertResult(1)(rs.leader(1))
    assertResult(2)(rs.leader(2))
    assertResult(0)(rs.leader(3))
    assertResult(1)(rs.leader(4))
    assertResult(2)(rs.leader(5))
    assertResult(0)(rs.leader(6))
    assertResult(1)(rs.leader(7))
    assertResult(2)(rs.leader(8))
  }

  it should "implement roundType correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    assertResult(FastRound)(rs.roundType(0))
    for (i <- 1 to 10) {
      assertResult(ClassicRound)(rs.roundType(i))
    }
  }

  it should "implement nextClassicRound correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)

    assertResult(3)(rs.nextClassicRound(leaderIndex = 0, round = -1))
    assertResult(3)(rs.nextClassicRound(leaderIndex = 0, round = 0))
    assertResult(3)(rs.nextClassicRound(leaderIndex = 0, round = 1))
    assertResult(3)(rs.nextClassicRound(leaderIndex = 0, round = 2))
    assertResult(6)(rs.nextClassicRound(leaderIndex = 0, round = 3))
    assertResult(6)(rs.nextClassicRound(leaderIndex = 0, round = 4))
    assertResult(6)(rs.nextClassicRound(leaderIndex = 0, round = 5))
    assertResult(9)(rs.nextClassicRound(leaderIndex = 0, round = 6))

    assertResult(1)(rs.nextClassicRound(leaderIndex = 1, round = -1))
    assertResult(1)(rs.nextClassicRound(leaderIndex = 1, round = 0))
    assertResult(4)(rs.nextClassicRound(leaderIndex = 1, round = 1))
    assertResult(4)(rs.nextClassicRound(leaderIndex = 1, round = 2))
    assertResult(4)(rs.nextClassicRound(leaderIndex = 1, round = 3))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 1, round = 4))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 1, round = 5))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 1, round = 6))

    assertResult(2)(rs.nextClassicRound(leaderIndex = 2, round = -1))
    assertResult(2)(rs.nextClassicRound(leaderIndex = 2, round = 0))
    assertResult(2)(rs.nextClassicRound(leaderIndex = 2, round = 1))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 2))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 3))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 4))
    assertResult(8)(rs.nextClassicRound(leaderIndex = 2, round = 5))
    assertResult(8)(rs.nextClassicRound(leaderIndex = 2, round = 6))
  }

  it should "implement nextFastRound correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    assertResult(Some(0))(rs.nextFastRound(0, -1))
    for (leader <- 0 to 3) {
      for (round <- 0 to 10) {
        assertResult(None)(rs.nextFastRound(leader, round))
      }
    }
  }

  "MixedRoundRobin" should "implement numLeaders correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    assertResult(3)(rs.numLeaders)
  }

  it should "implement leader correctly" in {
    val rs = new RoundSystem.MixedRoundRobin(3)
    assertResult(0)(rs.leader(0))
    assertResult(0)(rs.leader(1))
    assertResult(1)(rs.leader(2))
    assertResult(1)(rs.leader(3))
    assertResult(2)(rs.leader(4))
    assertResult(2)(rs.leader(5))
    assertResult(0)(rs.leader(6))
    assertResult(0)(rs.leader(7))
    assertResult(1)(rs.leader(8))
    assertResult(1)(rs.leader(9))
    assertResult(2)(rs.leader(10))
    assertResult(2)(rs.leader(11))
  }

  it should "implement roundType correctly" in {
    val rs = new RoundSystem.MixedRoundRobin(3)
    for (i <- 0 to 20 by 2) {
      assertResult(FastRound)(rs.roundType(i))
    }
    for (i <- 1 to 20 by 2) {
      assertResult(ClassicRound)(rs.roundType(i))
    }
  }

  it should "implement nextClassicRound correctly" in {
    val rs = new RoundSystem.MixedRoundRobin(3)

    assertResult(1)(rs.nextClassicRound(leaderIndex = 0, round = -1))
    assertResult(1)(rs.nextClassicRound(leaderIndex = 0, round = 0))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 0, round = 1))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 0, round = 2))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 0, round = 3))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 0, round = 4))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 0, round = 5))
    assertResult(7)(rs.nextClassicRound(leaderIndex = 0, round = 6))
    assertResult(13)(rs.nextClassicRound(leaderIndex = 0, round = 7))
    assertResult(13)(rs.nextClassicRound(leaderIndex = 0, round = 8))

    assertResult(3)(rs.nextClassicRound(leaderIndex = 1, round = -1))
    assertResult(3)(rs.nextClassicRound(leaderIndex = 1, round = 0))
    assertResult(3)(rs.nextClassicRound(leaderIndex = 1, round = 1))
    assertResult(3)(rs.nextClassicRound(leaderIndex = 1, round = 2))
    assertResult(9)(rs.nextClassicRound(leaderIndex = 1, round = 3))
    assertResult(9)(rs.nextClassicRound(leaderIndex = 1, round = 4))
    assertResult(9)(rs.nextClassicRound(leaderIndex = 1, round = 5))
    assertResult(9)(rs.nextClassicRound(leaderIndex = 1, round = 6))
    assertResult(9)(rs.nextClassicRound(leaderIndex = 1, round = 7))
    assertResult(9)(rs.nextClassicRound(leaderIndex = 1, round = 8))
    assertResult(15)(rs.nextClassicRound(leaderIndex = 1, round = 9))
    assertResult(15)(rs.nextClassicRound(leaderIndex = 1, round = 10))

    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = -1))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 0))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 1))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 2))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 3))
    assertResult(5)(rs.nextClassicRound(leaderIndex = 2, round = 4))
    assertResult(11)(rs.nextClassicRound(leaderIndex = 2, round = 5))
    assertResult(11)(rs.nextClassicRound(leaderIndex = 2, round = 6))
    assertResult(11)(rs.nextClassicRound(leaderIndex = 2, round = 7))
    assertResult(11)(rs.nextClassicRound(leaderIndex = 2, round = 8))
    assertResult(11)(rs.nextClassicRound(leaderIndex = 2, round = 9))
    assertResult(11)(rs.nextClassicRound(leaderIndex = 2, round = 10))
    assertResult(17)(rs.nextClassicRound(leaderIndex = 2, round = 11))
    assertResult(17)(rs.nextClassicRound(leaderIndex = 2, round = 12))
  }

  it should "implement nextFastRound correctly" in {
    val rs = new RoundSystem.MixedRoundRobin(3)

    assertResult(Some(0))(rs.nextFastRound(leaderIndex = 0, round = -1))
    assertResult(Some(6))(rs.nextFastRound(leaderIndex = 0, round = 0))
    assertResult(Some(6))(rs.nextFastRound(leaderIndex = 0, round = 1))
    assertResult(Some(6))(rs.nextFastRound(leaderIndex = 0, round = 2))
    assertResult(Some(6))(rs.nextFastRound(leaderIndex = 0, round = 3))
    assertResult(Some(6))(rs.nextFastRound(leaderIndex = 0, round = 4))
    assertResult(Some(6))(rs.nextFastRound(leaderIndex = 0, round = 5))
    assertResult(Some(12))(rs.nextFastRound(leaderIndex = 0, round = 6))
    assertResult(Some(12))(rs.nextFastRound(leaderIndex = 0, round = 7))

    assertResult(Some(2))(rs.nextFastRound(leaderIndex = 1, round = -1))
    assertResult(Some(2))(rs.nextFastRound(leaderIndex = 1, round = 0))
    assertResult(Some(2))(rs.nextFastRound(leaderIndex = 1, round = 1))
    assertResult(Some(8))(rs.nextFastRound(leaderIndex = 1, round = 2))
    assertResult(Some(8))(rs.nextFastRound(leaderIndex = 1, round = 3))
    assertResult(Some(8))(rs.nextFastRound(leaderIndex = 1, round = 4))
    assertResult(Some(8))(rs.nextFastRound(leaderIndex = 1, round = 5))
    assertResult(Some(8))(rs.nextFastRound(leaderIndex = 1, round = 6))
    assertResult(Some(8))(rs.nextFastRound(leaderIndex = 1, round = 7))
    assertResult(Some(14))(rs.nextFastRound(leaderIndex = 1, round = 8))
    assertResult(Some(14))(rs.nextFastRound(leaderIndex = 1, round = 9))

    assertResult(Some(4))(rs.nextFastRound(leaderIndex = 2, round = -1))
    assertResult(Some(4))(rs.nextFastRound(leaderIndex = 2, round = 0))
    assertResult(Some(4))(rs.nextFastRound(leaderIndex = 2, round = 1))
    assertResult(Some(4))(rs.nextFastRound(leaderIndex = 2, round = 2))
    assertResult(Some(4))(rs.nextFastRound(leaderIndex = 2, round = 3))
    assertResult(Some(10))(rs.nextFastRound(leaderIndex = 2, round = 4))
    assertResult(Some(10))(rs.nextFastRound(leaderIndex = 2, round = 5))
    assertResult(Some(10))(rs.nextFastRound(leaderIndex = 2, round = 6))
    assertResult(Some(10))(rs.nextFastRound(leaderIndex = 2, round = 7))
    assertResult(Some(10))(rs.nextFastRound(leaderIndex = 2, round = 8))
    assertResult(Some(10))(rs.nextFastRound(leaderIndex = 2, round = 9))
    assertResult(Some(16))(rs.nextFastRound(leaderIndex = 2, round = 10))
    assertResult(Some(16))(rs.nextFastRound(leaderIndex = 2, round = 11))
  }
}
