package frankenpaxos.simplebpaxos

import VertexIdHelpers.vertexIdOrdering
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._

object VertexIdPrefixSetBenchmark extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

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
}
