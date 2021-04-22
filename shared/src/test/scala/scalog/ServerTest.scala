package frankenpaxos.scalog

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

class ServerTest extends FlatSpec with Matchers with PropertyChecks {
  "Server.projectCut" should "project one server correctly" in {
    //   0   1   2   3   4   5   6   7   8   9
    // +---+---+---+---+---+---+---+---+---+---+
    // |   |   |   |   |   |   |   |   |   |   |
    // +---+---+---+---+---+---+---+---+---+---+
    //  \_/ \_____/ \_____/
    // 0 1     2       3
    val numServers = 1
    val serverIndex = 0
    val cuts = new frankenpaxos.util.BufferMap[Server.Cut]()
    cuts.put(0, Seq(0))
    cuts.put(1, Seq(1))
    cuts.put(2, Seq(3))
    cuts.put(3, Seq(5))
    cuts.put(5, Seq(8))

    Server.projectCut(numServers, serverIndex, cuts, 0) shouldBe
      Some(Server.Projection(0, 0, 0, 0))
    Server.projectCut(numServers, serverIndex, cuts, 1) shouldBe
      Some(Server.Projection(0, 1, 0, 1))
    Server.projectCut(numServers, serverIndex, cuts, 2) shouldBe
      Some(Server.Projection(1, 3, 1, 3))
    Server.projectCut(numServers, serverIndex, cuts, 3) shouldBe
      Some(Server.Projection(3, 5, 3, 5))
    Server.projectCut(numServers, serverIndex, cuts, 4) shouldBe None
    Server.projectCut(numServers, serverIndex, cuts, 5) shouldBe None
  }

  it should "project two servers correctly" in {
    //   0   1   2   3   4   5   6   7   8   9
    // +---+---+---+---+---+---+---+---+---+---+
    // | 0 | 1 | 1 | 0 | 1 | 1 |   |   |   |   |
    // +---+---+---+---+---+---+---+---+---+---+
    //  \_/ \_/ \_/ \_________/
    // 0 1   2   3       4
    val numServers = 2
    val cuts = new frankenpaxos.util.BufferMap[Server.Cut]()
    cuts.put(0, Seq(0, 0))
    cuts.put(1, Seq(1, 0))
    cuts.put(2, Seq(1, 1))
    cuts.put(3, Seq(1, 2))
    cuts.put(4, Seq(2, 4))
    cuts.put(6, Seq(5, 5))

    Server.projectCut(numServers, 0, cuts, 0) shouldBe
      Some(Server.Projection(0, 0, 0, 0))
    Server.projectCut(numServers, 1, cuts, 0) shouldBe
      Some(Server.Projection(0, 0, 0, 0))

    Server.projectCut(numServers, 0, cuts, 1) shouldBe
      Some(Server.Projection(0, 1, 0, 1))
    Server.projectCut(numServers, 1, cuts, 1) shouldBe
      Some(Server.Projection(1, 1, 0, 0))

    Server.projectCut(numServers, 0, cuts, 2) shouldBe
      Some(Server.Projection(1, 1, 1, 1))
    Server.projectCut(numServers, 1, cuts, 2) shouldBe
      Some(Server.Projection(1, 2, 0, 1))

    Server.projectCut(numServers, 0, cuts, 3) shouldBe
      Some(Server.Projection(2, 2, 1, 1))
    Server.projectCut(numServers, 1, cuts, 3) shouldBe
      Some(Server.Projection(2, 3, 1, 2))

    Server.projectCut(numServers, 0, cuts, 4) shouldBe
      Some(Server.Projection(3, 4, 1, 2))
    Server.projectCut(numServers, 1, cuts, 4) shouldBe
      Some(Server.Projection(4, 6, 2, 4))

    Server.projectCut(numServers, 0, cuts, 5) shouldBe None
    Server.projectCut(numServers, 1, cuts, 5) shouldBe None

    Server.projectCut(numServers, 0, cuts, 6) shouldBe None
    Server.projectCut(numServers, 1, cuts, 6) shouldBe None
  }

  it should "project four servers correctly" in {
    //   0   1   2   3   4   5   6   7   8   9
    // +---+---+---+---+---+---+---+---+---+---+
    // | 0 | 1 | 1 | 3 | 0 | 2 |   |   |   |   |
    // +---+---+---+---+---+---+---+---+---+---+
    //  \_/ \_________/ \_____/
    // 0 1       2         3
    val numServers = 4
    val cuts = new frankenpaxos.util.BufferMap[Server.Cut]()
    cuts.put(0, Seq(0, 0, 0, 0))
    cuts.put(1, Seq(1, 0, 0, 0))
    cuts.put(2, Seq(1, 2, 0, 1))
    cuts.put(3, Seq(2, 2, 1, 1))
    cuts.put(5, Seq(3, 3, 3, 3))

    Server.projectCut(numServers, 0, cuts, 0) shouldBe
      Some(Server.Projection(0, 0, 0, 0))
    Server.projectCut(numServers, 1, cuts, 0) shouldBe
      Some(Server.Projection(0, 0, 0, 0))
    Server.projectCut(numServers, 2, cuts, 0) shouldBe
      Some(Server.Projection(0, 0, 0, 0))
    Server.projectCut(numServers, 3, cuts, 0) shouldBe
      Some(Server.Projection(0, 0, 0, 0))

    Server.projectCut(numServers, 0, cuts, 1) shouldBe
      Some(Server.Projection(0, 1, 0, 1))
    Server.projectCut(numServers, 1, cuts, 1) shouldBe
      Some(Server.Projection(1, 1, 0, 0))
    Server.projectCut(numServers, 2, cuts, 1) shouldBe
      Some(Server.Projection(1, 1, 0, 0))
    Server.projectCut(numServers, 3, cuts, 1) shouldBe
      Some(Server.Projection(1, 1, 0, 0))

    Server.projectCut(numServers, 0, cuts, 2) shouldBe
      Some(Server.Projection(1, 1, 1, 1))
    Server.projectCut(numServers, 1, cuts, 2) shouldBe
      Some(Server.Projection(1, 3, 0, 2))
    Server.projectCut(numServers, 2, cuts, 2) shouldBe
      Some(Server.Projection(3, 3, 0, 0))
    Server.projectCut(numServers, 3, cuts, 2) shouldBe
      Some(Server.Projection(3, 4, 0, 1))

    Server.projectCut(numServers, 0, cuts, 3) shouldBe
      Some(Server.Projection(4, 5, 1, 2))
    Server.projectCut(numServers, 1, cuts, 3) shouldBe
      Some(Server.Projection(5, 5, 2, 2))
    Server.projectCut(numServers, 2, cuts, 3) shouldBe
      Some(Server.Projection(5, 6, 0, 1))
    Server.projectCut(numServers, 3, cuts, 3) shouldBe
      Some(Server.Projection(6, 6, 1, 1))

    Server.projectCut(numServers, 0, cuts, 4) shouldBe None
    Server.projectCut(numServers, 1, cuts, 4) shouldBe None
    Server.projectCut(numServers, 2, cuts, 4) shouldBe None
    Server.projectCut(numServers, 3, cuts, 4) shouldBe None

    Server.projectCut(numServers, 0, cuts, 5) shouldBe None
    Server.projectCut(numServers, 1, cuts, 5) shouldBe None
    Server.projectCut(numServers, 2, cuts, 5) shouldBe None
    Server.projectCut(numServers, 3, cuts, 5) shouldBe None
  }
}
