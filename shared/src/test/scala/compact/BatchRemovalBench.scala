package frankenpaxos.compact

import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._
import scala.collection.mutable

object BatchRemovalBenchmark extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  case class Params(
      numInserted: Int,
      numCollected: Int,
      numTrials: Int
  )

  performance of "Map --" in {
    val params = for {
      numInserted <- Gen.enumeration("numInserted")(100000)
      numCollected <- Gen.enumeration("numCollected")(25000,
                                                      50000,
                                                      75000,
                                                      95000)
      numTrials <- Gen.enumeration("numTrials")(10)
    } yield Params(numInserted, numCollected, numTrials)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      for (_ <- 0 until params.numTrials) {
        val map = mutable.Map[Int, Int]()
        for (i <- 0 until params.numInserted) {
          map(i) = i
        }
        for (i <- 0 until params.numInserted) {
          map(i)
        }
        map -- (0 until params.numCollected)
      }
    }
  }

  performance of "Map retain" in {
    val params = for {
      numInserted <- Gen.enumeration("numInserted")(100000)
      numCollected <- Gen.enumeration("numCollected")(25000,
                                                      50000,
                                                      75000,
                                                      95000)
      numTrials <- Gen.enumeration("numTrials")(10)
    } yield Params(numInserted, numCollected, numTrials)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      for (_ <- 0 until params.numTrials) {
        val map = mutable.Map[Int, Int]()
        for (i <- 0 until params.numInserted) {
          map(i) = i
        }
        for (i <- 0 until params.numInserted) {
          map(i)
        }
        map.retain({ case (k, _) => k >= params.numCollected })
      }
    }
  }

  performance of "SortedMap range" in {
    val params = for {
      numInserted <- Gen.enumeration("numInserted")(100000)
      numCollected <- Gen.enumeration("numCollected")(25000,
                                                      50000,
                                                      75000,
                                                      95000)
      numTrials <- Gen.enumeration("numTrials")(10)
    } yield Params(numInserted, numCollected, numTrials)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      for (_ <- 0 until params.numTrials) {
        var map = mutable.SortedMap[Int, Int]()
        for (i <- 0 until params.numInserted) {
          map(i) = i
        }
        for (i <- 0 until params.numInserted) {
          map(i)
        }
        map = map.from(params.numCollected)
      }
    }
  }

  performance of "Buffer trimStart" in {
    val params = for {
      numInserted <- Gen.enumeration("numInserted")(100000)
      numCollected <- Gen.enumeration("numCollected")(25000,
                                                      50000,
                                                      75000,
                                                      95000)
      numTrials <- Gen.enumeration("numTrials")(10)
    } yield Params(numInserted, numCollected, numTrials)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      for (_ <- 0 until params.numTrials) {
        var buffer = mutable.Buffer[Int]()
        for (i <- 0 until params.numInserted) {
          buffer += i
        }
        for (i <- 0 until params.numInserted) {
          buffer(i)
        }
        buffer.trimStart(params.numCollected)
      }
    }
  }
}
