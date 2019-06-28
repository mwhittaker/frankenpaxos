package frankenpaxos.unanimousbpaxos

import frankenpaxos.simulator.BadHistory
import frankenpaxos.simulator.Simulator
import org.scalatest.FlatSpec

class UnanimousBPaxosTest extends FlatSpec {
  "A UnanimousBPaxos instance" should "work correctly" in {
    val runLength = 100
    val numRuns = 500
    info(s"runLength = $runLength, numRuns = $numRuns")

    for (f <- 1 to 3) {
      val sim = new SimulatedUnanimousBPaxos(f)

      Simulator
        .simulate(sim, runLength = runLength, numRuns = numRuns)
        .flatMap(b => Simulator.minimize(sim, b.history)) match {
        case Some(BadHistory(history, throwable)) => {
          // https://stackoverflow.com/a/1149712/3187068
          val sw = new java.io.StringWriter()
          val pw = new java.io.PrintWriter(sw)
          throwable.printStackTrace(pw)

          val formatted_history = history.map(_.toString).mkString("\n")
          fail(s"$sw\n${sim.historyToString(history)}")
        }
        case None => {}
      }

      val suffix = s"f=$f"
      if (sim.valueChosen) {
        info(s"Value chosen ($suffix)")
      } else {
        info(s"No value chosen ($suffix)")
      }
    }
  }
}
