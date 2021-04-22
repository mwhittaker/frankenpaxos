package frankenpaxos.scalog

import frankenpaxos.simulator.BadHistory
import frankenpaxos.simulator.Simulator
import org.scalatest.FlatSpec

class ScalogTest extends FlatSpec {
  "A Scalog instance" should "work correctly" in {
    val runLength = 250
    val numRuns = 250
    info(s"runLength = $runLength, numRuns = $numRuns")

    for (f <- 1 to 3) {
      val sim = new SimulatedScalog(f = f)
      Simulator
        .simulate(sim, runLength = runLength * f, numRuns = numRuns)
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

      if (sim.valueChosen) {
        info(s"Value chosen (f=$f)")
      } else {
        info(s"No value chosen (f=$f)")
      }
    }
  }
}
