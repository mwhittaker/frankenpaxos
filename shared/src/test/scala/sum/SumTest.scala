package frankenpaxos.sum

import frankenpaxos.simulator.BadHistory
import frankenpaxos.simulator.Simulator
import org.scalatest.FlatSpec

class SumTest extends FlatSpec {
  "A Sum instance" should "work correctly" in {
    val runLength = 250
    val numRuns = 500
    info(s"runLength = $runLength, numRuns = $numRuns")

    val sim = new SimulatedSum()

    Simulator
      .simulate(sim, runLength = runLength, numRuns = numRuns)
      .flatMap(b => Simulator.minimize(sim, b.seed, b.history)) match {
      case Some(BadHistory(seed, history, throwable)) => {
        // https://stackoverflow.com/a/1149712/3187068
        val sw = new java.io.StringWriter()
        val pw = new java.io.PrintWriter(sw)
        throwable.printStackTrace(pw)

        val formatted_history = history.map(_.toString).mkString("\n")
        fail(s"Seed: $seed\n$sw\n${sim.historyToString(history)}")
      }
      case None => {}
    }
  }
}
