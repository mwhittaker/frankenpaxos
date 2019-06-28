package frankenpaxos.bankaccount

import frankenpaxos.simulator._
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed

class BankAccount {
  var balance: Int = 0

  def deposit(amount: Int): Unit = {
    balance += amount
  }

  def withdraw(amount: Int): Unit = {
    if (balance - amount < 0) {
      return
    }
    balance -= amount
  }
}

sealed trait BankAccountCommand
case class Deposit(amount: Int) extends BankAccountCommand
case class Withdraw(amount: Int) extends BankAccountCommand

class SimulatedBankAccount extends SimulatedSystem {
  override type System = BankAccount
  override type State = Int
  override type Command = BankAccountCommand

  override def newSystem(seed: Long): System = new BankAccount()

  override def getState(system: System): State = system.balance

  override def generateCommand(system: System): Option[Command] = {
    val gen: Gen[Command] =
      Gen.oneOf(
        Gen.choose(0, 100).map(Deposit(_)),
        Gen.choose(0, 100).map(Withdraw(_))
      )
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(system: System, command: Command): System = {
    command match {
      case Deposit(amount)  => system.deposit(amount)
      case Withdraw(amount) => system.withdraw(amount)
    }
    system
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    if (state < 0) {
      SimulatedSystem.InvariantViolated(
        s"Bank account balance $state is less than 0."
      )
    } else {
      SimulatedSystem.InvariantHolds
    }
  }
}
