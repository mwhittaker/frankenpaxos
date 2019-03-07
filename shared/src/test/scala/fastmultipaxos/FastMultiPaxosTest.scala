package frankenpaxos.fastmultipaxos

import frankenpaxos.simulator._
import org.scalatest._

class FastMultiPaxosSpec extends FlatSpec {
  "A FastMultiPaxos instance" should "work correctly" in {
    for (f <- 1 to 3) {
      val sim = new SimulatedFastMultiPaxos(f)
      Simulator
        .simulate(sim, runLength = 100, numRuns = 1000)
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
    }
  }
}
