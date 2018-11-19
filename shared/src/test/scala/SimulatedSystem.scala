package zeno

trait SimulatedSystem[Self <: SimulatedSystem[Self]] {
  type System
  type State
  type Command

  def newSystem(): Self#System

  def getState(system: Self#System): Self#State

  def invariantHolds(
      newState: Self#State,
      oldState: Option[Self#State]
  ): Option[String]

  def generateCommand(
      system: Self#System
  ): Option[Self#Command]

  def runCommand(
      system: Self#System,
      command: Self#Command
  ): Unit
}
