package frankenpaxos.util

import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._

object BufferMapBench extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  performance of "put" in {
    case class Params(
        n: Int,
        growSize: Int,
        numTrials: Int
    )

    val params =
      for {
        n <- Gen.enumeration("n")(100000)
        growSize <- Gen.enumeration("growSize")(100, 1000, 10000)
        numTrials <- Gen.enumeration("numTrials")(10)
      } yield Params(n, growSize, numTrials)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      for (_ <- 0 until params.numTrials) {
        val map = new BufferMap[Int](params.growSize)
        for (i <- 0 until params.n) {
          map.put(i, i)
        }
      }
    }
  }

  performance of "put then get" in {
    case class Params(
        n: Int,
        growSize: Int,
        numTrials: Int
    )

    val params =
      for {
        n <- Gen.enumeration("n")(100000)
        growSize <- Gen.enumeration("growSize")(100, 1000, 10000)
        numTrials <- Gen.enumeration("numTrials")(10)
      } yield Params(n, growSize, numTrials)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      for (_ <- 0 until params.numTrials) {
        val map = new BufferMap[Int](params.growSize)
        for (i <- 0 until params.n) {
          map.put(i, i)
        }
        for (i <- 0 until params.n) {
          map.get(i)
        }
      }
    }
  }

  performance of "garbageCollect" in {
    case class Params(
        n: Int,
        gcEvery: Int,
        growSize: Int,
        numTrials: Int
    )

    val params =
      for {
        n <- Gen.enumeration("n")(100000)
        gcEvery <- Gen.enumeration("gcEvery")(1, 10, 100, 1000, 10000)
        growSize <- Gen.enumeration("growSize")(10000)
        numTrials <- Gen.enumeration("numTrials")(10)
      } yield Params(n, gcEvery, growSize, numTrials)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      for (_ <- 0 until params.numTrials) {
        val map = new BufferMap[Int](params.growSize)
        for (i <- 0 until params.n) {
          map.put(i, i)
          if (i + 1 % params.gcEvery == 0) {
            map.garbageCollect(i + 1)
          }
        }
      }
    }
  }
}
