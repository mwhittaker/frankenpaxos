package zeno

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
      numRuns: Int = 100
  ): Option[(String, Seq[Sim#Command])] = {
    for (_ <- 1 to numRuns) {
      val history = simulateOne(sim, runLength)
      if (history.isDefined) {
        return history
      }
    }

    None
  }

  private def simulateOne[Sim <: SimulatedSystem[Sim]](
      sim: Sim,
      runLength: Int
  ): Option[(String, Seq[Sim#Command])] = {
    var history = Seq[Sim#Command]()
    var system = sim.newSystem()
    var oldState: Option[Sim#State] = None
    var newState = sim.getState(system)

    sim.invariantHolds(newState, oldState) match {
      case Some(error) => return Some(error, history)
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
        case Some(error) => return Some(error, history)
        case None        => {}
      }
    }

    None
  }

  // TODO(mwhittaker): Implement a history minimizer.
}
