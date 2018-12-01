package zeno.examples

import org.scalatest._
import zeno.BadHistory
import zeno.Simulator

class PaxosSpec extends FlatSpec {
  "A Paxos instance" should "only ever choose a single value" in {
    // TODO(mwhittaker): If a test case fails, show the log.
    // TODO(mwhittaker): Uniquely id each message.
    // TODO(mwhittaker): Minimize failing test cases.
    for (f <- 1 to 3) {
      val sim = new SimulatedPaxos(f)
      Simulator
        .simulate(sim, runLength = 100, numRuns = 1000)
        .flatMap(b => Simulator.minimize(sim, b.history, 10 * 1000)) match {
        case Some(BadHistory(history, error)) =>
          val formatted_history = history.map(_.toString).mkString("\n")
          fail(s"Error: $error\n$formatted_history")
        case None => {}
      }
    }
  }
}
