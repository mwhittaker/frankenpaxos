package frankenpaxos.depgraph

import frankenpaxos.simplebpaxos.VertexId
import frankenpaxos.simplebpaxos.VertexIdHelpers.vertexIdOrdering
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

object DependencyGraphBenchmark extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  val params =
    for {
      n <- Gen.enumeration("num_commands")(10000)
      cycleSize <- Gen.enumeration("cycle_size")(1, 5, 10)
    } yield (n, cycleSize)

  performance of "JgraphtDependencyGraph" in {
    using(params) config (
      exec.independentSamples -> 3,
      exec.benchRuns -> 5,
    ) in {
      case (n, cycleSize) =>
        val g = new JgraphtDependencyGraph[VertexId, Unit]()
        for (i <- 0 until n by cycleSize) {
          for (j <- 0 until cycleSize) {
            val deps = for (d <- i until i + cycleSize if d != i + j)
              yield VertexId(d, d)
            g.commit(VertexId(i + j, i + j), (), deps.toSet)
          }
        }
    }
  }
}
