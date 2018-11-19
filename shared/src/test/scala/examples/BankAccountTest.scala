package zeno.examples

import org.scalatest._
import zeno.Simulator

class BankAccountSpec extends FlatSpec {
  "A bank account" should "always be positive" in {
    Simulator.simulate(
      new SimulatedBankAccount(),
      runLength = 100,
      numRuns = 100
    ) match {
      case Some((error, history)) => fail(s"Error: $error\n$history")
      case None                   => {}
    }
  }
}
