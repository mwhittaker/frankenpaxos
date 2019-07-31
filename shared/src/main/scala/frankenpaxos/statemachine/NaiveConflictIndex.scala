package frankenpaxos.statemachine

import scala.collection.mutable

class NaiveConflictIndex[Key, Command](
    conflict: (Command, Command) => Boolean
) extends ConflictIndex[Key, Command] {
  private val commands = mutable.Map[Key, Command]()

  override def put(key: Key, command: Command): Option[Command] =
    commands.put(key, command)

  override def get(key: Key): Option[Command] =
    commands.get(key)

  override def remove(key: Key): Option[Command] =
    commands.remove(key)

  override def getConflicts(command: Command): Set[Key] = {
    commands
      .filter({ case (_, c) => conflict(c, command) })
      .keys
      .to[Set]
  }
}
