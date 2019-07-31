package frankenpaxos.statemachine

import frankenpaxos.compact.CompactSet
import frankenpaxos.compact.CompactSetFactory

class CompactedConflictIndex {}

// class CompactedConflictIndex[
//     Key,
//     Command,
//     KeySet <: CompactSet[KeySet] { type T = Key }
// ](
//     implicit keySetFactory: CompactSetFactory[KeySet]
// ) {
//   // Put, get, and remove follow Scala's Map API [1].
//   //
//   // [1]: scala-lang.org/api/current/scala/collection/mutable/Map.html
//   def put(key: Key, command: Command): Option[Command]
//   def get(key: Key): Option[Command]
//   def remove(key: Key): Option[Command]
//
//   // `getConflicts(key, command)` returns the set of all keys in the conflict
//   // index that map to commands that conflict with `command`. Note that `key`
//   // is never returned, even if it is in the conflict index.
//   def getConflicts(key: Key, command: Command): Set[Key]
// }
