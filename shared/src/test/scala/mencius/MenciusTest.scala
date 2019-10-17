package frankenpaxos.mencius

import frankenpaxos.simulator.BadHistory
import frankenpaxos.simulator.Simulator
import org.scalatest.FlatSpec

class MenciusTest extends FlatSpec {
  "A Mencius instance" should "work correctly" in {
    val runLength = 250
    val numRuns = 1000
    info(s"runLength = $runLength, numRuns = $numRuns")

    for {
      batched <- Seq(true, false)
      f <- 1 to 2
    } {
      val sim = new SimulatedMencius(f = f, batched = batched)

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

      val suffix = s"f=$f, batched=$batched"
      if (sim.valueChosen) {
        info(s"Value chosen ($suffix)")
      } else {
        info(s"No value chosen ($suffix)")
      }
    }
  }
}
