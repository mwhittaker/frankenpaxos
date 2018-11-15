package zeno.examples

import org.scalatest._
import zeno.Simulator

class PaxosSpec extends FlatSpec {

  "A Paxos instance" should "only ever choose a single value" in {
    for (f <- 1 to 3) {
      Simulator.simulate(
        new SimulatedPaxos(f),
        runLength = 50,
        numRuns = 100
      ) match {
        case Some(history) => fail(history.map(_.toString).mkString("\n"))
        case None          => {}
      }
    }
  }
}
