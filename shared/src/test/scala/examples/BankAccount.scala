package zeno.examples

import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import zeno.SimulatedSystem

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

class SimulatedBankAccount extends SimulatedSystem[SimulatedBankAccount] {
  override type System = BankAccount
  override type State = Int
  override type Command = BankAccountCommand

  override def newSystem(): SimulatedBankAccount#System = {
    new BankAccount()
  }

  override def getState(
      system: SimulatedBankAccount#System
  ): SimulatedBankAccount#State = {
    system.balance
  }

  override def invariantHolds(
      newState: SimulatedBankAccount#State,
      oldState: Option[SimulatedBankAccount#State]
  ): Option[String] = {
    if (newState < 0) {
      return Some(s"Bank account balance $newState is less than 0.")
    }

    None
  }

  override def generateCommand(
      system: SimulatedBankAccount#System
  ): Option[SimulatedBankAccount#Command] = {
    val gen: Gen[SimulatedBankAccount#Command] =
      Gen.oneOf(
        Gen.choose(0, 100).map(Deposit(_)),
        Gen.choose(0, 100).map(Withdraw(_))
      )
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(
      system: SimulatedBankAccount#System,
      command: SimulatedBankAccount#Command
  ): SimulatedBankAccount#System = {
    command match {
      case Deposit(amount)  => system.deposit(amount)
      case Withdraw(amount) => system.withdraw(amount)
    }
    system
  }
}
