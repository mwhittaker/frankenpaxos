package frankenpaxos.simulator

// A SimulatedSystem is a stateful system that can be property tested, kinda
// like QuickCheck for stateful systems. The design of SimulatedSystem was
// inspired by ScalaCheck's API [1].
//
// # Overview
//
// Property testing, like with QuickCheck, allows you to check whether a
// function satisfies an invariant. For example, if we had a function
// `reverse(xs: List[Int]): List[Int]`, we could check that for all lists `xs`,
// `reverse(reverse(xs)) == xs`. More concretely, we could express this
// invariant using ScalaCheck like this:
//
//   forAll { xs: List[String] => reverse(reverse(xs)) == xs }
//
// To check that a function f satisfies an invariant, property testing
// generates a lot of random inputs to f and checks that f satisfies the
// invariant for every generated input. For example, to check that reverse
// satisfies the invariant above, property testing would randomly generate
// 100's of lists `xs` and check that `reverse(reverse(xs)) == xs` for every
// one.
//
// This type of property testing works great for pure functions like `reverse`,
// but it doesn't work so well for stateful systems. SimulatedSystem is an
// abstraction that allows us to property test stateful systems.
//
// # An Example
//
// Imagine we have the following (trivial) stateful system that represents a
// single bank account. Money can be deposited into the bank account with the
// `deposit` method, and money can be withdrawn from the bank account using the
// `withdrawn` method so long as the balance does not drop below 0.
//
//   class BankAccount {
//     var balance: Int = 0
//
//     def deposit(amount: Int): Unit = {
//       balance += amount
//     }
//
//     def withdraw(amount: Int): Unit = {
//       if (balance - amount < 0) {
//         return
//       }
//       balance -= amount
//     }
//   }
//
// We want to test that for any sequence of deposit and withdraw requests, the
// account balance remains positive. To do so, we create a subclass of
// SimulatedSystem that we can use to property test the BankAccount class. It
// looks something like this:
//
//   // First, we define a type to represent the possible commands that our
//   // BankAccount supports. Our BankAccount supports deposits and withdraws,
//   // so we create Deposit and Withdraw case classes.
//   sealed trait BankAccountCommand
//   case class Deposit(amount: Int) extends BankAccountCommand
//   case class Withdraw(amount: Int) extends BankAccountCommand
//
//   // Next, we define our SimulatedBankAccount class. The SimulatedSystem
//   // trait uses F-bounded polymorphism so SimulatedBankAccount extends
//   // SimulatedSystem[SimulatedBankAccount].
//   class SimulatedBankAccount extends SimulatedSystem[SimulatedBankAccount] {
//     // Every simulated system has to define three types. System is the type
//     // of the system being tested. Here, it's the bank account class.
//     override type System = BankAccount
//
//     // State is the relevant state of our system over which our invariant is
//     // defined. You should make State an immutable type. Here, we choose
//     // State to be the integer balance in the bank account.
//     override type State = Int
//
//     // Finally, we specify the type of Command that we'll run against our
//     // system. We use the BankAccountCommand type that we defined above.
//     override type Command = BankAccountCommand
//
//     // newSystem returns a new instantiation of our system.
//     override def newSystem(): SimulatedBankAccount#System = {
//       new BankAccount()
//     }
//
//     // getState extracts the state from our system.
//     override def getState(
//         system: SimulatedBankAccount#System
//     ): SimulatedBankAccount#State = {
//       system.balance
//     }
//
//     // invariantHolds returns None if the invariant holds and Some(error) if
//     // the invariant does not hold. invariantHolds takes in the current state
//     // and previous state of the system to make it easier to test invariants
//     // that deal with state transitions (e.g., account balance only goes up).
//     override def invariantHolds(
//         newState: SimulatedBankAccount#State,
//         oldState: Option[SimulatedBankAccount#State]
//     ): Option[String] = {
//       if (newState < 0) {
//         return Some(s"Bank account balance $newState is less than 0.")
//       }
//
//       None
//     }
//
//     // generateCommand returns a randomly generated command that can be
//     // applied to the state machine. If no such command exists (e.g., the
//     // system has halted and cannot process any further commands), then
//     // generateCommand should return None.
//     override def generateCommand(
//         system: SimulatedBankAccount#System
//     ): Option[SimulatedBankAccount#Command] = {
//       val gen: Gen[SimulatedBankAccount#Command] =
//         Gen.oneOf(
//           Gen.choose(0, 100).map(Deposit(_)),
//           Gen.choose(0, 100).map(Withdraw(_))
//         )
//       gen.apply(Gen.Parameters.default, Seed.random())
//     }
//
//     // runCommand runs a particular command through a system.
//     override def runCommand(
//         system: SimulatedBankAccount#System,
//         command: SimulatedBankAccount#Command
//     ): SimulatedBankAccount#System = {
//       command match {
//         case Deposit(amount)  => system.deposit(amount)
//         case Withdraw(amount) => system.withdraw(amount)
//       }
//       system
//     }
//   }
//
// Once we've defined a SimulatedSystem, we can property test it with the
// Simulator object. See Simulator.scala for more information. See
// examples/BankAccount.scala and examples/BankAccountTest.scala for an
// executable version of the example above.
//
// # Why not ScalaCheck?
//
// ScalaCheck comes with an API for stateful property testing [1], so why not
// use that? With the ScalaCheck API, every command has to define how to
// transition from one state to the next. ScalaCheck then checks that these
// state transitions match the actual state transitions that the system takes.
//
// Defining these state transitions for complex systems like Paxos is not easy.
// Defining the state transitions is as hard as implementing the system. Unlike
// this API, SimulatedSystem does not require you to write a state transition
// specification.
//
// [1] github.com/rickynils/scalacheck/blob/master/doc/UserGuide.md#stateful-testing
trait SimulatedSystem {
  type System
  type State
  type Command

  def newSystem(): System

  def getState(system: System): State

  def invariantHolds(newState: State, oldState: Option[State]): Option[String]

  def generateCommand(system: System): Option[Command]

  // TODO(mwhittaker): Note that if the command is illegal, the state should be
  // unchanged. This allows us to look at subsets of failing test cases.
  def runCommand(system: System, command: Command): System
}
