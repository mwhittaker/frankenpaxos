package zeno

import org.scalatest._

class BankAccountSpec extends FlatSpec {

  "A bank account" should "always be positive" in {
    Simulator.simulate(
      new SimulatedBankAccount(),
      runLength = 100,
      numRuns = 100
    ) match {
      case Some(history) => fail(history.toString())
      case None          => {}
    }
  }
}
