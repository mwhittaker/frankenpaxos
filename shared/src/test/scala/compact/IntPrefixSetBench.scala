package frankenpaxos.compact

import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._

object IntPrefixSetBenchmark extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  performance of "diff" in {
    case class Params(
        lhs: Array[Byte],
        rhs: Array[Byte],
        numDiffs: Int
    )

    val params =
      for {
        lhs <- Gen.enumeration("lhs")(
          (0, Set[Int]()),
          (10, Set[Int]()),
          (1000, Set[Int]())
        )
        rhs <- Gen.enumeration("rhs")(
          (0, Set[Int]()),
          (10, Set[Int]()),
          (1000, Set[Int]())
        )
        numDiffs <- Gen.enumeration("numDiffs")(10000)
      } yield
        Params(
          IntPrefixSet(lhs._1, lhs._2).toProto.toByteArray,
          IntPrefixSet(rhs._1, rhs._2).toProto.toByteArray,
          numDiffs
        )

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val lhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.lhs))
      val rhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.rhs))
      for (_ <- 0 until params.numDiffs) {
        lhs.diff(rhs)
      }
    }
  }
}
