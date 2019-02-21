package frankenpaxos.simulator

import org.scalatest._

class BankAccountSpec extends FlatSpec {
  "A bank account" should "always be positive" in {
    val sim = new SimulatedBankAccount()
    Simulator
      .simulate(sim, runLength = 100, numRuns = 100)
      .flatMap(b => Simulator.minimize(sim, b.history)) match {
      case Some(BadHistory(history, error)) =>
        fail(s"Error: $error\n$history")
      case None => {}
    }
  }
}
