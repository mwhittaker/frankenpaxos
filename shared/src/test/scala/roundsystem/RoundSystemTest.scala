package frankenpaxos.roundsystem

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class RoundSystemSpec extends FlatSpec with Matchers {
  // ClassicRoundRobin. ////////////////////////////////////////////////////////
  "ClassicRoundRobin" should "implement numLeaders correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)
    rs.numLeaders shouldBe 3
  }

  it should "implement leader correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)
    rs.leader(0) shouldBe 0
    rs.leader(1) shouldBe 1
    rs.leader(2) shouldBe 2
    rs.leader(3) shouldBe 0
    rs.leader(4) shouldBe 1
    rs.leader(5) shouldBe 2
    rs.leader(6) shouldBe 0
    rs.leader(7) shouldBe 1
    rs.leader(8) shouldBe 2
  }

  it should "implement roundType correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)
    for (i <- 0 until 10) {
      rs.roundType(i) shouldBe ClassicRound
    }
  }

  it should "implement nextClassicRound correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 0
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 6
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 6
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 6
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 9

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 4
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 4
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 4
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 7

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 2
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 8
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 8
  }

  it should "implement nextFastRound correctly" in {
    val rs = new RoundSystem.ClassicRoundRobin(3)
    for (leader <- 0 until 3) {
      for (round <- -1 until 10) {
        rs.nextFastRound(leader, round) shouldBe None
      }
    }
  }

  // RoundZeroFast. ////////////////////////////////////////////////////////////
  "RoundZeroFast" should "implement numLeaders correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    rs.numLeaders shouldBe 3
  }

  it should "implement leader correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    rs.leader(0) shouldBe 0
    rs.leader(1) shouldBe 0
    rs.leader(2) shouldBe 1
    rs.leader(3) shouldBe 2
    rs.leader(4) shouldBe 0
    rs.leader(5) shouldBe 1
    rs.leader(6) shouldBe 2
    rs.leader(7) shouldBe 0
    rs.leader(8) shouldBe 1
  }

  it should "implement roundType correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    rs.roundType(0) shouldBe FastRound
    for (i <- 1 until 10) {
      rs.roundType(i) shouldBe ClassicRound
    }
  }

  it should "implement nextClassicRound correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 4
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 4
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 4
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 7

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 2
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 8
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 8

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 6
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 6
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 6
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 9
  }

  it should "implement nextFastRound correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    rs.nextFastRound(0, -1) shouldBe Some(0)
    for (leader <- 0 until 3) {
      for (round <- 0 until 10) {
        rs.nextFastRound(leader, round) shouldBe None
      }
    }
  }

  // MixedRoundRobin. //////////////////////////////////////////////////////////
  "MixedRoundRobin" should "implement numLeaders correctly" in {
    val rs = new RoundSystem.RoundZeroFast(3)
    rs.numLeaders shouldBe 3
  }

  it should "implement leader correctly" in {
    val rs = new RoundSystem.MixedRoundRobin(3)
    rs.leader(0) shouldBe 0
    rs.leader(1) shouldBe 0
    rs.leader(2) shouldBe 1
    rs.leader(3) shouldBe 1
    rs.leader(4) shouldBe 2
    rs.leader(5) shouldBe 2
    rs.leader(6) shouldBe 0
    rs.leader(7) shouldBe 0
    rs.leader(8) shouldBe 1
    rs.leader(9) shouldBe 1
    rs.leader(10) shouldBe 2
    rs.leader(11) shouldBe 2
  }

  it should "implement roundType correctly" in {
    val rs = new RoundSystem.MixedRoundRobin(3)
    for (i <- 0 until 20 by 2) {
      rs.roundType(i) shouldBe FastRound
    }
    for (i <- 1 until 20 by 2) {
      rs.roundType(i) shouldBe ClassicRound
    }
  }

  it should "implement nextClassicRound correctly" in {
    val rs = new RoundSystem.MixedRoundRobin(3)

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 7) shouldBe 13
    rs.nextClassicRound(leaderIndex = 0, round = 8) shouldBe 13

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 9
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 9
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 9
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 9
    rs.nextClassicRound(leaderIndex = 1, round = 7) shouldBe 9
    rs.nextClassicRound(leaderIndex = 1, round = 8) shouldBe 9
    rs.nextClassicRound(leaderIndex = 1, round = 9) shouldBe 15
    rs.nextClassicRound(leaderIndex = 1, round = 10) shouldBe 15

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 11
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 11
    rs.nextClassicRound(leaderIndex = 2, round = 7) shouldBe 11
    rs.nextClassicRound(leaderIndex = 2, round = 8) shouldBe 11
    rs.nextClassicRound(leaderIndex = 2, round = 9) shouldBe 11
    rs.nextClassicRound(leaderIndex = 2, round = 10) shouldBe 11
    rs.nextClassicRound(leaderIndex = 2, round = 11) shouldBe 17
    rs.nextClassicRound(leaderIndex = 2, round = 12) shouldBe 17
  }

  it should "implement nextFastRound correctly" in {
    val rs = new RoundSystem.MixedRoundRobin(3)

    rs.nextFastRound(leaderIndex = 0, round = -1) shouldBe Some(0)
    rs.nextFastRound(leaderIndex = 0, round = 0) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 1) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 2) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 3) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 4) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 5) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 6) shouldBe Some(12)
    rs.nextFastRound(leaderIndex = 0, round = 7) shouldBe Some(12)

    rs.nextFastRound(leaderIndex = 1, round = -1) shouldBe Some(2)
    rs.nextFastRound(leaderIndex = 1, round = 0) shouldBe Some(2)
    rs.nextFastRound(leaderIndex = 1, round = 1) shouldBe Some(2)
    rs.nextFastRound(leaderIndex = 1, round = 2) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 1, round = 3) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 1, round = 4) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 1, round = 5) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 1, round = 6) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 1, round = 7) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 1, round = 8) shouldBe Some(14)
    rs.nextFastRound(leaderIndex = 1, round = 9) shouldBe Some(14)

    rs.nextFastRound(leaderIndex = 2, round = -1) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 2, round = 0) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 2, round = 1) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 2, round = 2) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 2, round = 3) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 2, round = 4) shouldBe Some(10)
    rs.nextFastRound(leaderIndex = 2, round = 5) shouldBe Some(10)
    rs.nextFastRound(leaderIndex = 2, round = 6) shouldBe Some(10)
    rs.nextFastRound(leaderIndex = 2, round = 7) shouldBe Some(10)
    rs.nextFastRound(leaderIndex = 2, round = 8) shouldBe Some(10)
    rs.nextFastRound(leaderIndex = 2, round = 9) shouldBe Some(10)
    rs.nextFastRound(leaderIndex = 2, round = 10) shouldBe Some(16)
    rs.nextFastRound(leaderIndex = 2, round = 11) shouldBe Some(16)
  }

  // RenamedRoundSystem. ///////////////////////////////////////////////////////
  "RenamedRoundSystem" should "implement numLeaders correctly" in {
    val rs = new RoundSystem.RenamedRoundSystem(
      new RoundSystem.MixedRoundRobin(3),
      Map(0 -> 0, 1 -> 2, 2 -> 1)
    )
    rs.numLeaders shouldBe 3
  }

  it should "implement leader correctly" in {
    val rs = new RoundSystem.RenamedRoundSystem(
      new RoundSystem.MixedRoundRobin(3),
      Map(0 -> 0, 1 -> 2, 2 -> 1)
    )
    rs.leader(0) shouldBe 0
    rs.leader(1) shouldBe 0
    rs.leader(2) shouldBe 2
    rs.leader(3) shouldBe 2
    rs.leader(4) shouldBe 1
    rs.leader(5) shouldBe 1
    rs.leader(6) shouldBe 0
    rs.leader(7) shouldBe 0
    rs.leader(8) shouldBe 2
  }

  it should "implement roundType correctly" in {
    val rs = new RoundSystem.RenamedRoundSystem(
      new RoundSystem.MixedRoundRobin(3),
      Map(0 -> 0, 1 -> 2, 2 -> 1)
    )
    rs.roundType(0) shouldBe FastRound
    rs.roundType(1) shouldBe ClassicRound
    rs.roundType(2) shouldBe FastRound
    rs.roundType(3) shouldBe ClassicRound
    rs.roundType(4) shouldBe FastRound
    rs.roundType(5) shouldBe ClassicRound
    rs.roundType(6) shouldBe FastRound
    rs.roundType(7) shouldBe ClassicRound
    rs.roundType(8) shouldBe FastRound
  }

  it should "implement nextClassicRound correctly" in {
    val rs = new RoundSystem.RenamedRoundSystem(
      new RoundSystem.MixedRoundRobin(3),
      Map(0 -> 0, 1 -> 2, 2 -> 1)
    )

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 7

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 11
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 11

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 9
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 9
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 9
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 9
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 9
  }

  it should "implement nextFastRound correctly" in {
    val rs = new RoundSystem.RenamedRoundSystem(
      new RoundSystem.MixedRoundRobin(3),
      Map(0 -> 0, 1 -> 2, 2 -> 1)
    )

    rs.nextFastRound(leaderIndex = 0, round = -1) shouldBe Some(0)
    rs.nextFastRound(leaderIndex = 0, round = 0) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 1) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 2) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 3) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 4) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 5) shouldBe Some(6)
    rs.nextFastRound(leaderIndex = 0, round = 6) shouldBe Some(12)

    rs.nextFastRound(leaderIndex = 1, round = -1) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 1, round = 0) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 1, round = 1) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 1, round = 2) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 1, round = 3) shouldBe Some(4)
    rs.nextFastRound(leaderIndex = 1, round = 4) shouldBe Some(10)
    rs.nextFastRound(leaderIndex = 1, round = 5) shouldBe Some(10)
    rs.nextFastRound(leaderIndex = 1, round = 6) shouldBe Some(10)

    rs.nextFastRound(leaderIndex = 2, round = -1) shouldBe Some(2)
    rs.nextFastRound(leaderIndex = 2, round = 0) shouldBe Some(2)
    rs.nextFastRound(leaderIndex = 2, round = 1) shouldBe Some(2)
    rs.nextFastRound(leaderIndex = 2, round = 2) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 2, round = 3) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 2, round = 4) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 2, round = 5) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 2, round = 6) shouldBe Some(8)
    rs.nextFastRound(leaderIndex = 2, round = 6) shouldBe Some(8)
  }

  // RotatedRoundSystem. ///////////////////////////////////////////////////////
  private def rotatedRoundSystems(): Seq[RoundSystem.RotatedRoundSystem] = {
    Seq(
      new RoundSystem.RotatedRoundSystem(new RoundSystem.ClassicRoundRobin(3),
                                         -3),
      new RoundSystem.RotatedRoundSystem(new RoundSystem.ClassicRoundRobin(3),
                                         3),
      new RoundSystem.RotatedRoundSystem(new RoundSystem.ClassicRoundRobin(3),
                                         6)
    )
  }

  "RotatedRoundSystem" should "implement numLeaders correctly" in {
    for (rs <- rotatedRoundSystems()) {
      rs.numLeaders shouldBe 3
    }
  }

  it should "implement leader correctly" in {
    for (rs <- rotatedRoundSystems()) {
      rs.leader(0) shouldBe 0
      rs.leader(1) shouldBe 1
      rs.leader(2) shouldBe 2
      rs.leader(3) shouldBe 0
      rs.leader(4) shouldBe 1
      rs.leader(5) shouldBe 2
      rs.leader(6) shouldBe 0
      rs.leader(7) shouldBe 1
      rs.leader(8) shouldBe 2
    }
  }

  it should "implement roundType correctly" in {
    for (rs <- rotatedRoundSystems()) {
      for (i <- 0 until 10) {
        rs.roundType(i) shouldBe ClassicRound
      }
    }
  }

  it should "implement nextClassicRound correctly" in {
    for (rs <- rotatedRoundSystems()) {
      rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 0
      rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 3
      rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 3
      rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 3
      rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 6
      rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 6
      rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 6
      rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 9

      rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 1
      rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 1
      rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 4
      rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 4
      rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 4
      rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 7
      rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 7
      rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 7

      rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 2
      rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 2
      rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 2
      rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 5
      rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 5
      rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 5
      rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 8
      rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 8
    }
  }

  it should "implement nextFastRound correctly" in {
    for (rs <- rotatedRoundSystems()) {
      for (leader <- 0 until 3) {
        for (round <- -1 until 10) {
          rs.nextFastRound(leader, round) shouldBe None
        }
      }
    }
  }

  // RotatedClassicRoundRobin. /////////////////////////////////////////////////
  "RotatedClassicRoundRobin" should "implement numLeaders correctly" in {
    for (firstLeader <- 0 until 3) {
      val rs = new RoundSystem.RotatedClassicRoundRobin(3, firstLeader)
      rs.numLeaders shouldBe 3
    }
  }

  it should "implement leader correctly" in {
    var rs = new RoundSystem.RotatedClassicRoundRobin(3, 0)
    rs.leader(0) shouldBe 0
    rs.leader(1) shouldBe 1
    rs.leader(2) shouldBe 2
    rs.leader(3) shouldBe 0
    rs.leader(4) shouldBe 1
    rs.leader(5) shouldBe 2
    rs.leader(6) shouldBe 0
    rs.leader(7) shouldBe 1
    rs.leader(8) shouldBe 2

    rs = new RoundSystem.RotatedClassicRoundRobin(3, 1)
    rs.leader(0) shouldBe 1
    rs.leader(1) shouldBe 2
    rs.leader(2) shouldBe 0
    rs.leader(3) shouldBe 1
    rs.leader(4) shouldBe 2
    rs.leader(5) shouldBe 0
    rs.leader(6) shouldBe 1
    rs.leader(7) shouldBe 2
    rs.leader(8) shouldBe 0

    rs = new RoundSystem.RotatedClassicRoundRobin(3, 2)
    rs.leader(0) shouldBe 2
    rs.leader(1) shouldBe 0
    rs.leader(2) shouldBe 1
    rs.leader(3) shouldBe 2
    rs.leader(4) shouldBe 0
    rs.leader(5) shouldBe 1
    rs.leader(6) shouldBe 2
    rs.leader(7) shouldBe 0
    rs.leader(8) shouldBe 1
  }

  it should "implement roundType correctly" in {
    for (firstLeader <- 1 until 3) {
      val rs = new RoundSystem.RotatedClassicRoundRobin(3, firstLeader)
      for (i <- 0 until 10) {
        rs.roundType(i) shouldBe ClassicRound
      }
    }
  }

  it should "implement nextClassicRound correctly" in {
    var rs = new RoundSystem.RotatedClassicRoundRobin(3, 0)

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 0
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 6
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 6
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 6
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 9

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 4
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 4
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 4
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 7

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 2
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 8
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 8

    rs = new RoundSystem.RotatedClassicRoundRobin(3, 1)

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 2
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 8
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 8

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 0
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 6
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 6
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 6
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 9

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 4
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 4
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 4
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 7

    rs = new RoundSystem.RotatedClassicRoundRobin(3, 2)

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 4
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 4
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 4
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 7

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 2
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 8
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 8

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 0
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 6
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 6
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 6
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 9
  }

  it should "implement nextFastRound correctly" in {
    for (firstLeader <- 0 until 3) {
      val rs = new RoundSystem.RotatedClassicRoundRobin(3, firstLeader)
      for (leader <- 0 until 3) {
        for (round <- -1 until 10) {
          rs.nextFastRound(leader, round) shouldBe None
        }
      }
    }
  }

  // RotatedRoundZeroFast. /////////////////////////////////////////////////////
  "RotatedRoundZeroFast" should "implement numLeaders correctly" in {
    for (firstLeader <- 0 until 3) {
      val rs = new RoundSystem.RotatedRoundZeroFast(3, firstLeader)
      rs.numLeaders shouldBe 3
    }
  }

  it should "implement leader correctly" in {
    var rs = new RoundSystem.RotatedRoundZeroFast(3, 0)
    rs.leader(0) shouldBe 0
    rs.leader(1) shouldBe 0
    rs.leader(2) shouldBe 1
    rs.leader(3) shouldBe 2
    rs.leader(4) shouldBe 0
    rs.leader(5) shouldBe 1
    rs.leader(6) shouldBe 2
    rs.leader(7) shouldBe 0
    rs.leader(8) shouldBe 1

    rs = new RoundSystem.RotatedRoundZeroFast(3, 1)
    rs.leader(0) shouldBe 1
    rs.leader(1) shouldBe 1
    rs.leader(2) shouldBe 2
    rs.leader(3) shouldBe 0
    rs.leader(4) shouldBe 1
    rs.leader(5) shouldBe 2
    rs.leader(6) shouldBe 0
    rs.leader(7) shouldBe 1
    rs.leader(8) shouldBe 2

    rs = new RoundSystem.RotatedRoundZeroFast(3, 2)
    rs.leader(0) shouldBe 2
    rs.leader(1) shouldBe 2
    rs.leader(2) shouldBe 0
    rs.leader(3) shouldBe 1
    rs.leader(4) shouldBe 2
    rs.leader(5) shouldBe 0
    rs.leader(6) shouldBe 1
    rs.leader(7) shouldBe 2
    rs.leader(8) shouldBe 0
  }

  it should "implement roundType correctly" in {
    for (firstLeader <- 0 until 3) {
      val rs = new RoundSystem.RotatedRoundZeroFast(3, firstLeader)
      rs.roundType(0) shouldBe FastRound
      for (i <- 1 until 10) {
        rs.roundType(i) shouldBe ClassicRound
      }
    }
  }

  it should "implement nextClassicRound correctly" in {
    var rs = new RoundSystem.RotatedRoundZeroFast(3, 0)

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 4
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 4
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 4
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 7

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 2
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 8
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 8

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 6
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 6
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 6
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 9

    rs = new RoundSystem.RotatedRoundZeroFast(3, 1)

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 6
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 6
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 6
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 9

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 4
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 4
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 4
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 7

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 2
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 8
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 8

    rs = new RoundSystem.RotatedRoundZeroFast(3, 2)

    rs.nextClassicRound(leaderIndex = 0, round = -1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 0, round = 0) shouldBe 2
    rs.nextClassicRound(leaderIndex = 0, round = 1) shouldBe 2
    rs.nextClassicRound(leaderIndex = 0, round = 2) shouldBe 5
    rs.nextClassicRound(leaderIndex = 0, round = 3) shouldBe 5
    rs.nextClassicRound(leaderIndex = 0, round = 4) shouldBe 5
    rs.nextClassicRound(leaderIndex = 0, round = 5) shouldBe 8
    rs.nextClassicRound(leaderIndex = 0, round = 6) shouldBe 8

    rs.nextClassicRound(leaderIndex = 1, round = -1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 0) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 1) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 2) shouldBe 3
    rs.nextClassicRound(leaderIndex = 1, round = 3) shouldBe 6
    rs.nextClassicRound(leaderIndex = 1, round = 4) shouldBe 6
    rs.nextClassicRound(leaderIndex = 1, round = 5) shouldBe 6
    rs.nextClassicRound(leaderIndex = 1, round = 6) shouldBe 9

    rs.nextClassicRound(leaderIndex = 2, round = -1) shouldBe 1
    rs.nextClassicRound(leaderIndex = 2, round = 0) shouldBe 1
    rs.nextClassicRound(leaderIndex = 2, round = 1) shouldBe 4
    rs.nextClassicRound(leaderIndex = 2, round = 2) shouldBe 4
    rs.nextClassicRound(leaderIndex = 2, round = 3) shouldBe 4
    rs.nextClassicRound(leaderIndex = 2, round = 4) shouldBe 7
    rs.nextClassicRound(leaderIndex = 2, round = 5) shouldBe 7
    rs.nextClassicRound(leaderIndex = 2, round = 6) shouldBe 7
  }

  it should "implement nextFastRound correctly" in {
    for (firstLeader <- 0 until 3) {
      val rs = new RoundSystem.RotatedRoundZeroFast(3, firstLeader)
      rs.nextFastRound(firstLeader, -1) shouldBe Some(0)
      for (leader <- 0 until 3) {
        for (round <- 1 until 10) {
          rs.nextFastRound(leader, round) shouldBe None
        }
      }
    }
  }
}
