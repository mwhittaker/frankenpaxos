package frankenpaxos.statemachine

import frankenpaxos.util.TopK
import frankenpaxos.util.TopOne
import frankenpaxos.util.VertexIdLike

// Two state machine commands x and y conflict if there exists a state in which
// x and y do not commute; i.e. there exists a state in which executing x and
// then y does not produce the same final state and outputs as executing y and
// then x.
//
// Consider a key-value store state machine for example. The commands set(x, 1)
// and set(y, 1) do _not_ conflict because in every single state, executing
// set(x, 1) and then set(y, 1) is indistinguishable from executing set(y, 1)
// and then set(x, 1). On the other hand, the commands set(x, 1) and set(x, 2)
// do conflict. Executing set(x, 1) and then set(x, 2) leaves x with value 2
// while executing set(x, 2) and then set(x, 1) leaves x with value 1.
//
// Many consensus protocols (e.g., EPaxos, BPaxos) receive commands over time
// and assign each command a unique key (e.g., every command in EPaxos is
// assigned a particular instance). Moreover, whenever these protocols receive
// a command, they have to compute the set of already received commands that
// conflict with it. For example, imagine a consensus protocol receives the
// following stream of commands:
//
//   - key 1: set(x, 1)
//   - key 2: set(y, 1)
//   - key 3: get(x)
//   - key 4: get(y)
//
// and then receives command set(x, 2). The protocol computes the set of
// commands that conflict with set(x, 2). Here, the conflicting commands are
// set(x, 1) (key 1) and get(x) (key 3).
//
// ConflictIndex is an abstraction that can be used to efficiently compute sets
// of commands that conflict with a particular command. You can think of a
// ConflictIndex like a Map from keys to commands. You can put, get, and remove
// commands from a ConflictIndex. What differentiates a ConflictIndex from a
// Map is the getConflicts method which returns the set of all commands
// (technically, their keys) that conflict with a command. Read through a
// couple of the ConflictIndex implementations below to get a better sense for
// the API.
trait ConflictIndex[Key, Command] {
  // Put a command into the conflict index.
  def put(key: Key, command: Command): Unit

  // Put a snapshot into the conflict index. A snapshot conflicts with every
  // command, including other snapshots.
  def putSnapshot(key: Key): Unit

  // Remove a command or snapshot from the conflict index.
  def remove(key: Key): Unit =
    throw new java.lang.UnsupportedOperationException()

  // Return all conflicts.
  def getConflicts(command: Command): Set[Key] =
    throw new java.lang.UnsupportedOperationException()
  // Return top one conflicts.
  def getTopOneConflicts(command: Command): TopOne[Key] =
    throw new java.lang.UnsupportedOperationException()
  // Return top-k conflicts.
  def getTopKConflicts(command: Command): TopK[Key] =
    throw new java.lang.UnsupportedOperationException()
}
