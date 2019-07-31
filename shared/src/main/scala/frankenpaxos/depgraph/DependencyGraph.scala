package frankenpaxos.depgraph

import frankenpaxos.util
import scala.scalajs.js.annotation.JSExportAll

// # MultiPaxos
// MultiPaxos replicas agree on a log by committing one log entry at a time.
// Initially, the log is empty:
//
//       0   1   2   3   4   5
//     +---+---+---+---+---+---+
//     |   |   |   |   |   |   |
//     +---+---+---+---+---+---+
//
// Over time, log entries are committed. Here, we illustrate that a log entry
// is committed by drawing an X in it. This is what it looks like when log
// entry 2 is committed:
//
//       0   1   2   3   4   5
//     +---+---+---+---+---+---+
//     |   |   | X |   |   |   |
//     +---+---+---+---+---+---+
//
// When a log entry is committed and all previous log entries are committed, we
// say the log entry is _eligible_ for execution. Replicas execute log
// entries in increasing order: 0, then 1, then 2, and so on. Of course, a
// replica can only execute a log entry if it is eligible. In the example
// above, no log entries are eligible yet. Imagine log entry 0 is committed
// next:
//
//       0   1   2   3   4   5
//     +---+---+---+---+---+---+
//     | X |   | X |   |   |   |
//     +---+---+---+---+---+---+
//
// 0 is eligible and is executed. Then, imagine log entry 1 is committed.
//
//       0   1   2   3   4   5
//     +---+---+---+---+---+---+
//     | X | X | X |   |   |   |
//     +---+---+---+---+---+---+
//
// Log entries 1 and 2 are now eligible and can be executed (in order).
//
// # EPaxos, BPaxos, etc.
// Instead of agreeing on a _log_ one _entry_ at a time, some state machine
// replication protocols (like EPaxos and BPaxos) agree on a _graph_ one
// _vertex_ at a time. Initially, the graph is empty. Over time, vertices are
// committed and added to the graph. For example, imagine vertex B is committed
// with an edge to vertex A. That looks like this.
//
//
//       +---+
//     A | X |
//       +---+
//        |
//        |
//        v
//       +---+
//     B |   |
//       +---+
//
// Note that vertex A is committed, but vertex B is not. We say a vertex is
// _eligible_ for execution if it and every vertex reachable from it is
// committed. In the example above, no vertex is eligible. Now, imagine that
// vertex C is committed with an edge to A.
//
//       +---+      +---+
//     A | X |<-----| X | C
//       +---+      +---+
//        |
//        |
//        v
//       +---+
//     B |   |
//       +---+
//
// Still, no vertex is eligible. Now, imagine vertex B is committed with an
// edge to A.
//
//       +---+      +---+
//     A | X |<-----| X | C
//       +---+      +---+
//        | ^
//        | |
//        v |
//       +---+
//     B | X |
//       +---+
//
// Now, all three vertices are eligible. Like how MultiPaxos executes log
// entries in prefix order, protocols like EPaxos and BPaxos execute commands
// in "prefix order". More carefully, they execute strongly connected
// components of eligible commands in reverse topological order.
//
// In the example above, vertices A and B form a strongly connected component
// and vertex C forms a strongly connected component. Thus, the protocol
// executes the A,B component and then the C component. Within a component, the
// protocol is free to execute commands in an arbitrary order that respects
// sequence numbers (we omitted sequence numbers in this example to keep things
// simple). For example, if both A and B have the same sequence number, then
// EPaxos can execute commands in either of the following orders:
//
//   - A, B, C
//   - B, A, C
//
// but NOT in any of the following orders:
//
//   - C, A, B
//   - C, B, A
//   - A, C, B
//   - B, C, A
//
// All implementations of DependencyGraph here return commands within a
// component sorted first by sequence number and then by vertex key. So, the
// above graph would be executed as A, B, C.
//
// # API
// Note that vertex identifiers and sequence numbers are both generic. This is
// because EPaxos and BPaxos use different types of vertex identifiers, and
// BPaxos doesn't even use sequence numbers. The key and sequence number
// orderings are used to execute commands within a component in a deterministic
// order.
@JSExportAll
abstract class DependencyGraph[
    Key,
    SequenceNumber,
    KeySet <: util.CompactSet[KeySet] { type T = Key }
](
    implicit val keyOrdering: Ordering[Key],
    implicit val sequenceNumberOrdering: Ordering[SequenceNumber]
) {
  // Commit adds a vertex, its sequence number, and its dependencies to the
  // dependency graph. It does not try to execute any part of the graph.
  def commit(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: KeySet
  ): Unit

  // Execute finds vertices in the graph that are eligible for execution, and
  // then returns them in an order that is compatible with the graph (i.e. a
  // reverse topological order of components, with a deterministic ordering of
  // commands within components). Note that a dependency graph is not
  // guaranteed to return a component that is eligible as soon as it is
  // eligible.
  //
  // It is the responsibility of the caller of execute to execute the commands
  // associated with the vertices. Once a dependency graph returns a command
  // from execute, it will never return it again.
  def execute(): Seq[Key] = {
    for {
      component <- executeByComponent()
      key <- component
    } yield key
  }

  // executeByComponent is the same as execute, except that strongly connected
  // components are returned in their own Seq. This is mostly useful for
  // testing.
  def executeByComponent(): Seq[Seq[Key]]

  // Returns the current number of vertices in the graph. This is used mainly
  // for monitoring. A dependency graph implementation may or may not prune
  // vertices from the graph after they are executed, so these numbers may go
  // up and down over time.
  def numVertices: Int
}

object DependencyGraph {
  def read[K, S, KSet <: util.CompactSet[KSet] { type T = K }](
      implicit keyOrdering: Ordering[K],
      sequenceNumberOrdering: Ordering[S]
  ): scopt.Read[(KSet) => DependencyGraph[K, S, KSet]] = {
    scopt.Read.reads({
      case "Jgrapht" =>
        emptyKeySet => new JgraphtDependencyGraph[K, S, KSet](emptyKeySet)
      case "ScalaGraph" =>
        emptyKeySet => new ScalaGraphDependencyGraph[K, S, KSet](emptyKeySet)
      case "Tarjan" =>
        emptyKeySet => new TarjanDependencyGraph[K, S, KSet](emptyKeySet)
      case "IncrementalTarjan" =>
        emptyKeySet =>
          new IncrementalTarjanDependencyGraph[K, S, KSet](emptyKeySet)
      case x =>
        throw new IllegalArgumentException(
          s"$x is not one of Jgrapht, ScalaGraph, Tarjan, or IncrementalTarjan."
        )
    })
  }
}
