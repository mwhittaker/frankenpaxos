package zeno.examples

import org.scalatest._
import zeno.Simulator

class PaxosSpec extends FlatSpec {
  "A Paxos instance" should "only ever choose a single value" in {
    // TODO(mwhittaker): If a test case fails, show the log.
    // TODO(mwhittaker): Uniquely id each message.
    // TODO(mwhittaker): Minimize failing test cases.
    for (f <- 1 to 3) {
      Simulator.simulate(
        new SimulatedPaxos(f),
        runLength = 100,
        numRuns = 100
      ) match {
        case Some((error, history)) =>
          val formatted_history = history.map(_.toString).mkString("\n")
          fail(s"Error: $error\n$formatted_history")
        case None => {}
      }
    }
  }
}
