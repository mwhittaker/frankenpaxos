package frankenpaxos.fastmultipaxos

import frankenpaxos.simulator.BadHistory
import frankenpaxos.simulator.Simulator
import org.scalatest.FlatSpec

class FastMultiPaxosSpec extends FlatSpec {
  val runLength = 100
  val numRuns = 250

  def test(sim: SimulatedFastMultiPaxos): Unit = {
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
  }

  "A FastMultiPaxos instance" should "work correctly" in {
    info(s"runLength = $runLength, numRuns = $numRuns")

    for (f <- 1 to 3) {
      for (roundSystem <- Seq(new RoundSystem.ClassicRoundRobin(f + 1),
                              new RoundSystem.RoundZeroFast(f + 1),
                              new RoundSystem.MixedRoundRobin(f + 1))) {
        val sim = new SimulatedFastMultiPaxos(f, roundSystem)
        test(sim)

        val suffix = s"f=$f, roundSystem=$roundSystem"
        if (sim.valueChosen) {
          info(s"Value chosen ($suffix)")
        } else {
          info(s"No value chosen ($suffix)")
        }
      }
    }
  }
}
