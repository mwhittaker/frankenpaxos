package frankenpaxos.quorums

import com.google.protobuf.ByteString
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks
import scala.collection.mutable

class QuorumSystemTest extends FlatSpec {
  private def testAll(test: QuorumSystem[Int] => Unit): Unit = {
    for (i <- 1 until 10) {
      val qs = new SimpleMajority(Set() ++ (0 until i))
      test(qs)
      info(s"SimpleMajority $i passed.")
    }

    for (i <- 1 until 10) {
      val qs = new UnanimousWrites(Set() ++ (0 until i))
      test(qs)
      info(s"UnanimousWrites $i passed.")
    }
  }

  "A quorum system" should "have intersecting read and write quorums" in {
    def test(qs: QuorumSystem[Int]): Unit = {
      for (_ <- 0 until 100) {
        val readQuorum = qs.randomReadQuorum()
        val writeQuorum = qs.randomWriteQuorum()
        assert(
          !readQuorum.intersect(writeQuorum).isEmpty,
          s"Read quorum $readQuorum and write quorum $writeQuorum do not " +
            s"intersect."
        )
      }
    }
    testAll(test)
  }

  it should "compute read quorums that are read quorums" in {
    def test(qs: QuorumSystem[Int]): Unit = {
      for (_ <- 0 until 100) {
        val quorum = qs.randomReadQuorum()
        assert(qs.isReadQuorum(quorum),
               s"Read quorum $quorum is not a read quorum.")
      }
    }
    testAll(test)
  }

  it should "compute write quorums that are write quorums" in {
    def test(qs: QuorumSystem[Int]): Unit = {
      for (_ <- 0 until 100) {
        val quorum = qs.randomWriteQuorum()
        assert(qs.isWriteQuorum(quorum),
               s"Write quorum $quorum is not a write quorum.")
      }
    }
    testAll(test)
  }
}
