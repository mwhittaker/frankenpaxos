package spaxosdecouple

import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.simulator.{BadHistory, Simulator}
import org.scalatest.FlatSpec

class SPaxosDecoupleSpec extends FlatSpec {
  val runLength = 1000
  val numRuns = 500

  def test(sim: SimulatedSPaxosDecouple): Unit = {
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

  "A SPaxosDecouple instance" should "work correctly" in {
    info(s"runLength = $runLength, numRuns = $numRuns")

    for (f <- 1 to 7) {
      for (roundSystem <- Seq(new RoundSystem.ClassicRoundRobin(f + 1))) {
        val sim = new SimulatedSPaxosDecouple(f, roundSystem)
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
