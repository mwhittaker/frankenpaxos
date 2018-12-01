package zeno

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
      run: Seq[Sim#Command],
      numRuns: Int
  ): Option[BadHistory[Sim]] = {
    minimizeImpl(
      sim,
      run,
      MinimizeState(
        runBudget = numRuns,
        examinedRuns = Set[Seq[Sim#Command]](),
        badHistory = runOne(sim, run).map(BadHistory(run, _))
      )
    ).badHistory
  }

  private def strictSubsequences[A](xs: Seq[A]): Iterator[Seq[A]] = {
    // https://stackoverflow.com/a/39726711/3187068
    (xs.length - 1 to 0 by -1).toIterator.flatMap(xs.combinations(_))
  }

  private def minOption[A](
      lhs: Option[A],
      rhs: Option[A],
      min: (A, A) => A
  ): Option[A] = {
    (lhs, rhs) match {
      case (Some(_), None)    => lhs
      case (None, Some(_))    => rhs
      case (None, None)       => None
      case (Some(x), Some(y)) => Some(min(x, y))
    }
  }

  private def smallerBadHistory[Sim <: SimulatedSystem[Sim]](
      lhs: Option[BadHistory[Sim]],
      rhs: Option[BadHistory[Sim]]
  ): Option[BadHistory[Sim]] = {
    minOption[BadHistory[Sim]](
      lhs,
      rhs,
      (x, y) =>
        if (x.history.size <= y.history.size) {
          x
        } else {
          y
        }
    )
  }

  case class MinimizeState[Sim <: SimulatedSystem[Sim]](
      runBudget: Int,
      examinedRuns: Set[Seq[Sim#Command]],
      badHistory: Option[BadHistory[Sim]]
  )

  private def minimizeImpl[Sim <: SimulatedSystem[Sim]](
      sim: Sim,
      run: Seq[Sim#Command],
      _state: MinimizeState[Sim]
  ): MinimizeState[Sim] = {
    // We make _state a var because we are going to modify it in the loop below.
    var state = _state

    for (subrun <- strictSubsequences(run)) breakable {
      if (state.runBudget <= 0) {
        return state
      }

      if (state.examinedRuns.contains(subrun)) {
        // This break is actually a continue.
        break
      }

      // Run the subrun and update our state accordingly.
      val errorOption = runOne(sim, subrun)
      state = state.copy(
        runBudget = state.runBudget - 1,
        examinedRuns = state.examinedRuns + subrun
      )

      errorOption match {
        // If the subrun does _not_ result in an invariant violation, then we
        // move on to the next subrun.
        case None => {}

        // If the subrun _does_ result in an invariant violation, then we try
        // to minimize this subrun further.
        case Some(error) => {
          // First, we replace our current bad history, if this subrun results
          // in a smaller bad history.
          state = state.copy(
            badHistory = smallerBadHistory(
              state.badHistory,
              Some(BadHistory(subrun, error))
            )
          )

          // Then, we recurse.
          state = minimizeImpl(sim, subrun, state)
        }
      }
    }

    state
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

  // TODO(mwhittaker): Implement a history minimizer.
}
