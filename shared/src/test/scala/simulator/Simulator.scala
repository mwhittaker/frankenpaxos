package frankenpaxos.simulator

import org.scalacheck.Gen
import org.scalacheck.Prop
import org.scalacheck.Test
import util.control.Breaks._

// A BadHistory for a particular simulated system is a history that causes an
// invariant violation. That is, if the simulated system executes the history,
// its invariant will be violated. A BadHistory also includes the error
// message that results from the invariant violation.
case class BadHistory[Sim <: SimulatedSystem[Sim]](
    history: Seq[Sim#Command],
    error: String
)

// Simulator can be used to property test a SimulatedSystem using the
// `simulate` function. See SimulatedSystem.scala for information on defining
// SimulatedSystems.
object Simulator {
  // `simulate(sim, runLength, numRuns)` runs `numRuns` simulations of the
  // simulated system `sim`. Each simulation is at most `runLength` commands
  // long. After every step of every simulation, the invariant of the system is
  // checked. If the invariant does not hold, an error message and the sequence
  // of commands that led to the invariant violation are returned.
  def simulate[Sim <: SimulatedSystem[Sim]](
      sim: Sim,
      runLength: Int,
      numRuns: Int
  ): Option[BadHistory[Sim]] = {
    for (_ <- 1 to numRuns) {
      val badHistory = simulateOne(sim, runLength)
      if (badHistory.isDefined) {
        return badHistory
      }
    }

    None
  }

  def minimize[Sim <: SimulatedSystem[Sim]](
      sim: Sim,
      run: Seq[Sim#Command]
  ): Option[BadHistory[Sim]] = {
    // We check that every subrun of `run` is a good history (i.e. a history
    // that does not lead to an invariant violation). Of course, `run` should
    // be a bad history, so we know that this property is not true. Scalacheck
    // will find a minimal subsequence of `run` that also violates the
    // invariant.
    val prop = Prop.forAll(Gen.someOf(run)) { subrun =>
      runOne(sim, subrun).isEmpty
    }

    Test.check(Test.Parameters.default, prop) match {
      case Test.Result(Test.Failed(arg :: _, _), _, _, _, _) => {
        val subrun = arg.arg.asInstanceOf[Seq[Sim#Command]]
        Some(BadHistory(subrun, runOne(sim, subrun).get))
      }
      case _ => None
    }
  }

  private def simulateOne[Sim <: SimulatedSystem[Sim]](
      sim: Sim,
      runLength: Int
  ): Option[BadHistory[Sim]] = {
    var history = Seq[Sim#Command]()
    var system = sim.newSystem()
    var oldState: Option[Sim#State] = None
    var newState = sim.getState(system)

    sim.invariantHolds(newState, oldState) match {
      case Some(error) => return Some(BadHistory(history, error))
      case None        => {}
    }

    for (_ <- 1 to runLength) {
      val command = sim.generateCommand(system) match {
        case Some(c) => c
        case None    => return None
      }
      history = history :+ command
      system = sim.runCommand(system, command)
      oldState = Some(newState)
      newState = sim.getState(system)

      sim.invariantHolds(newState, oldState) match {
        case Some(error) => return Some(BadHistory(history, error))
        case None        => {}
      }
    }

    None
  }

  private def runOne[Sim <: SimulatedSystem[Sim]](
      sim: Sim,
      run: Seq[Sim#Command]
  ): Option[String] = {
    var system = sim.newSystem()
    var oldState: Option[Sim#State] = None
    var newState = sim.getState(system)

    var error = sim.invariantHolds(newState, oldState)
    if (error.isDefined) {
      return error
    }

    for (command <- run) {
      system = sim.runCommand(system, command)
      oldState = Some(newState)
      newState = sim.getState(system)

      error = sim.invariantHolds(newState, oldState)
      if (error.isDefined) {
        return error
      }
    }

    return None
  }
}
