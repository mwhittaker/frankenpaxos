package frankenpaxos.simulator

import org.scalacheck.Gen
import org.scalacheck.Prop
import org.scalacheck.Test

// A BadHistory for a particular simulated system is a history that causes
// either an invariant violation or leads to an exception being thrown. That
// is, if the simulated system executes the history, its invariant will be
// violated or an exception will be thrown. A BadHistory also includes a
// Throwable that explains what went wrong.
case class BadHistory[+Sim <: SimulatedSystem](
    history: Seq[Sim#Command],
    throwable: Throwable
)

// Simulator can be used to property test a SimulatedSystem using the
// `simulate` function. See SimulatedSystem.scala for information on defining
// SimulatedSystems.
object Simulator {
  // `simulate(sim, runLength, numRuns)` runs `numRuns` simulations of the
  // simulated system `sim`. Each simulation is at most `runLength` commands
  // long. After every step of every simulation, the invariant of the system is
  // checked. If the invariant does not hold, the run is minimized, and then an
  // error message and the sequence of commands that led to the invariant
  // violation are returned.
  def simulate[Sim <: SimulatedSystem](
      sim: Sim,
      runLength: Int,
      numRuns: Int
  ): Option[BadHistory[Sim]] = {
    for (_ <- 1 to numRuns) {
      simulateOne(sim, runLength) match {
        case badHistory @ Some(_) => return badHistory
        case None                 =>
      }
    }

    None
  }

  def minimize[Sim <: SimulatedSystem { type Command = C }, C](
      sim: Sim,
      run: Seq[C]
  ): Option[BadHistory[Sim]] = {
    // We check that every subrun of `run` is a good history (i.e. a history
    // that does not lead to an invariant violation). Of course, `run` should
    // be a bad history, so we know that this property is not true. Scalacheck
    // will find a minimal subsequence of `run` that also violates the
    // invariant.
    val prop = Prop.forAll(Gen.someOf(run)) { subrun =>
      runOne[sim.type, sim.Command](sim, subrun).isSuccess
    }

    val params = Test.Parameters.default
      .withMinSuccessfulTests(1500)
      .withWorkers(Runtime.getRuntime().availableProcessors())
    Test.check(params, prop) match {
      case Test.Result(Test.Failed(arg :: _, _), _, _, _, _) => {
        val subrun = arg.arg.asInstanceOf[Seq[sim.Command]]
        val throwable = runOne[sim.type, sim.Command](sim, subrun).failed.get
        Some(BadHistory(subrun, throwable))
      }
      case _ =>
        None
    }
  }

  private def simulateOne[Sim <: SimulatedSystem](
      sim: Sim,
      runLength: Int
  ): Option[BadHistory[Sim]] = {
    var history = Seq[sim.Command]()
    var system = sim.newSystem()
    var states = Seq[sim.State](sim.getState(system))

    checkInvariants[sim.type, sim.State](sim, states) match {
      case SimulatedSystem.InvariantViolated(explanation) =>
        return Some(BadHistory(history, new IllegalStateException(explanation)))

      case SimulatedSystem.InvariantHolds =>
        // Nothing to do.
        ()
    }

    for (_ <- 1 to runLength) {
      val command = sim.generateCommand(system) match {
        case Some(c) => c
        case None    => return None
      }
      history = history :+ command
      system = util.Try(sim.runCommand(system, command)) match {
        case util.Success(system) => system
        case util.Failure(throwable) =>
          return Some(BadHistory(history, throwable))
      }
      states = states :+ sim.getState(system)

      checkInvariants[sim.type, sim.State](sim, states) match {
        case SimulatedSystem.InvariantViolated(explanation) =>
          return Some(
            BadHistory(history, new IllegalStateException(explanation))
          )

        case SimulatedSystem.InvariantHolds =>
          // Nothing to do.
          ()
      }
    }

    None
  }

  // Run a simulated system `sim` on a particular run `run`. If the run is
  // successful---i.e., no invariants are violated and no exceptions are
  // thrown---then util.Success(()) is returned. Otherwise, util.Failure is
  // returned.
  private def runOne[Sim <: SimulatedSystem { type Command = C }, C](
      sim: Sim,
      run: Seq[C]
  ): util.Try[Unit] = {
    util.Try(runOneImpl[sim.type, C](sim, run))
  }

  // Same as runOne, but throws an exception if an invariant is violated.
  private def runOneImpl[Sim <: SimulatedSystem { type Command = C }, C](
      sim: Sim,
      run: Seq[C]
  ): Unit = {
    var system = sim.newSystem()
    var states = Seq[sim.State](sim.getState(system))

    checkInvariants[sim.type, sim.State](sim, states) match {
      case SimulatedSystem.InvariantViolated(explanation) =>
        throw new IllegalStateException(explanation)

      case SimulatedSystem.InvariantHolds =>
        // Nothing to do.
        ()
    }

    for (command <- run) {
      system = sim.runCommand(system, command)
      states = states :+ sim.getState(system)

      checkInvariants[sim.type, sim.State](sim, states) match {
        case SimulatedSystem.InvariantViolated(explanation) =>
          throw new IllegalStateException(explanation)

        case SimulatedSystem.InvariantHolds =>
          // Nothing to do.
          ()
      }
    }

    util.Success(())
  }

  // checkInvariants(sim, states) checks that the most recent states in
  // `states` satisfy `sim`'s invariants. That is,
  //
  //   - sim's state invariant is checked only on the last state;
  //   - sim's step invariant is checked only on the last step;
  //   - and sim's history invariant is checked on the entire history of
  //     states.
  //
  // Note that checkInvariants does not check the state invariant on every
  // state and does not check the step invariant on every step. However, by
  // invoking checkInvariants on a growing history each time a new state is
  // added, we can ensure that the history maintains all invariants.
  private def checkInvariants[Sim <: SimulatedSystem { type State = S }, S](
      sim: Sim,
      states: Seq[S]
  ): SimulatedSystem.InvariantResult = {
    states.takeRight(2) match {
      case Seq() => SimulatedSystem.InvariantHolds

      case Seq(state) =>
        sim
          .stateInvariantHolds(state)
          .and(sim.historyInvariantHolds(states))

      case Seq(oldState, newState) =>
        sim
          .stateInvariantHolds(newState)
          .and(sim.stepInvariantHolds(oldState, newState))
          .and(sim.historyInvariantHolds(states))

      case _ =>
        throw new IllegalStateException(
          "takeRight(2) returned more than 2 things!"
        )
    }
  }
}
