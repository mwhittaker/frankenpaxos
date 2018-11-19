package zeno

object Simulator {
  private def simulateOne[Sim <: SimulatedSystem[Sim]](
      sim: Sim,
      runLength: Int
  ): Option[(String, Seq[Sim#Command])] = {
    var history = Seq[Sim#Command]()
    val system = sim.newSystem()
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
      sim.runCommand(system, command)
      oldState = Some(newState)
      newState = sim.getState(system)

      sim.invariantHolds(newState, oldState) match {
        case Some(error) => return Some(error, history)
        case None        => {}
      }
    }

    None
  }

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

  // TODO(mwhittaker): Implement a history minimizer.
}
