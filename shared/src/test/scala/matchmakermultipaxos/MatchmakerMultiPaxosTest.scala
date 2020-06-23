package frankenpaxos.matchmakermultipaxos

import frankenpaxos.simulator.BadHistory
import frankenpaxos.simulator.Simulator
import org.scalatest.FlatSpec

class MatchmakerMultiPaxosTest extends FlatSpec {
  "A MatchmakerMultiPaxos instance" should "work correctly" in {
    val runLength = 250
    val numRuns = 100
    info(s"runLength = $runLength, numRuns = $numRuns")

    for {
      (f, stallDuringMatchmaking, stallDuringPhase1, disableGc) <- Seq(
        (1, false, false, false),
        (1, true, false, false),
        (1, false, true, false),
        (1, true, true, false),
        (1, true, true, true),
        (2, false, false, false)
      )
    } {
      val sim = new SimulatedMatchmakerMultiPaxos(
        f = f,
        stallDuringMatchmaking = stallDuringMatchmaking,
        stallDuringPhase1 = stallDuringPhase1,
        disableGc = disableGc
      )

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

      val suffix = s"(f=$f, stallDuringMatchmaking=$stallDuringMatchmaking, " +
        s"stallDuringPhase1=$stallDuringPhase1, disableGc=$disableGc)"
      if (sim.valueChosen) {
        info(s"Value chosen $suffix")
      } else {
        info(s"No value chosen $suffix")
      }
    }
  }
}
