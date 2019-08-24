package frankenpaxos.simplegcbpaxos

import VertexIdHelpers.vertexIdOrdering
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._

object VertexIdPrefixSetBenchmark extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  performance of "apply from set" in {
    case class Params(
        numLeaders: Int,
        numIdsPerLeader: Int,
        numTrials: Int
    )

    val params =
      for {
        numLeaders <- Gen.enumeration("numLeaders")(10)
        numIdsPerLeader <- Gen.enumeration("numIdsPerLeader")(0, 10)
        numTrials <- Gen.enumeration("numTrials")(100000)
      } yield Params(numLeaders, numIdsPerLeader, numTrials)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 10,
    ) in { params =>
      val factory = VertexIdPrefixSet.factory(params.numLeaders)
      val vertexIds = {
        for {
          i <- 0 until params.numLeaders
          j <- 0 until params.numIdsPerLeader
        } yield VertexId(i, j)
      }.toSet

      for (_ <- 0 until params.numTrials) {
        factory.fromSet(vertexIds)
      }
    }
  }

  performance of "add" in {
    case class Params(
        numLeaders: Int,
        numCommandsPerLeader: Int
    )

    val params =
      for {
        numLeaders <- Gen.enumeration("numLeaders")(5)
        numCommandsPerLeader <- Gen.enumeration("numCommandsPerLeader")(100000)
      } yield Params(numLeaders, numCommandsPerLeader)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val vertexIds = VertexIdPrefixSet(params.numLeaders)
      for (leader <- 0 until params.numLeaders) {
        for (i <- 0 until params.numCommandsPerLeader) {
          vertexIds.add(VertexId(leader, i))
        }
      }
    }
  }

  performance of "union of empty sets" in {
    case class Params(
        numLeaders: Int,
        numTrials: Int
    )

    val params =
      for {
        numLeaders <- Gen.enumeration("numLeaders")(10)
        numTrials <- Gen.enumeration("numTrials")(100000)
      } yield Params(numLeaders, numTrials)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val a = VertexIdPrefixSet(params.numLeaders)
      val b = VertexIdPrefixSet(params.numLeaders)
      val c = VertexIdPrefixSet(params.numLeaders)
      for (_ <- 0 until params.numTrials) {
        a.union(b).union(c)
      }
    }
  }
}
