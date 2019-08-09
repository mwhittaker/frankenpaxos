package frankenpaxos.compact

import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._

object IntPrefixSetBenchmark extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  performance of "add" in {
    case class Params(numAdd: Int)

    val params = for (numAdd <- Gen.enumeration("numAdd")(100000))
      yield Params(numAdd)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val prefixSet = IntPrefixSet()
      for (i <- 0 until params.numAdd) {
        prefixSet.add(i)
      }
    }
  }

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
          (20, Set[Int]()),
          (1500, Set[Int]())
        )
        numDiffs <- Gen.enumeration("numDiffs")(100000)
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

  performance of "union" in {
    case class Params(
        lhs: Array[Byte],
        rhs: Array[Byte],
        numUnions: Int
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
        numUnions <- Gen.enumeration("numUnions")(100000)
      } yield
        Params(
          IntPrefixSet(lhs._1, lhs._2).toProto.toByteArray,
          IntPrefixSet(rhs._1, rhs._2).toProto.toByteArray,
          numUnions
        )

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val lhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.lhs))
      val rhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.rhs))
      for (_ <- 0 until params.numUnions) {
        lhs.union(rhs)
      }
    }
  }

  performance of "addAll" in {
    case class Params(
        lhs: Array[Byte],
        rhs: Array[Byte],
        numAddAlls: Int
    )

    val params =
      for {
        lhs <- Gen.enumeration("lhs")(
          (0, 0, 0),
          (1000, 0, 0),
          (0, 100, 200),
          (1000, 1600, 1700)
        )
        rhs <- Gen.enumeration("rhs")(
          (0, 0, 0),
          (1500, 0, 0),
          (0, 150, 250),
          (1500, 1650, 1750)
        )
        numAddAlls <- Gen.enumeration("numAddAlls")(100000)
      } yield
        Params(
          IntPrefixSet(lhs._1, (lhs._2 until lhs._3).toSet).toProto.toByteArray,
          IntPrefixSet(rhs._1, (rhs._2 until rhs._3).toSet).toProto.toByteArray,
          numAddAlls
        )

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val lhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.lhs))
      val rhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.rhs))
      for (_ <- 0 until params.numAddAlls) {
        lhs.addAll(rhs)
      }
    }
  }
}
