package frankenpaxos.epaxos

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
// Log entry 1 and 2 are now eligible and can be executed (in order).
//
// # EPaxos
// Instead of agreeing on a log one entry at a time, EPaxos replicas agree on a
// graph one vertex at a time. Initially, the graph is empty. Over time,
// vertices are committed and added to the graph. For example, imagine vertex B
// is committed with an edge to vertex A. That looks like this.
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
// entries in prefix order, EPaxos executes commands in "prefix order". More
// carefully, it executes strongly connected components of eligible commands in
// reverse topological order.
//
// In the example above, vertices A and B form a strongly connected component
// and vertex C forms a strongly connected component. Thus, EPaxos executes the
// A,B component and then the C component. Within a component, EPaxos is free
// to execute commands in an arbitrary order that respects sequence numbers (we
// ommitted sequence numbers in this example to keep things simple). For
// example, if both A and B have the same sequence number, then EPaxos can
// execute commands in either of the following orders:
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
// # API
// DependencyGraph represents the dependency graphs maintained by EPaxos. A
// DependencyGraph has a single method, `commit`, to commit a vertex
// (identified by an instance), its sequence number, and its dependencies.
// `commit` returns a set of instances that can be executed, arranged in the
// order that they should be executed. For example, we can replicate the
// example above like this:
//
//   val A = Instance(...)
//   val B = Instance(...)
//   val C = Instance(...)
//
//   val g = ... // Make a DependencyGraph.
//   g.commit(A, 0, Set(B)) // Evaluates to Seq()
//   g.commit(C, 0, Set(A)) // Evaluates to Seq()
//   g.commit(B, 0, Set(A)) // Evaluates to Seq(A, B, C) or Seq(B, A, C)
//
// TODO(mwhittaker): Abstract and pull out DependencyGraph from the epaxos
// package so that we can use it for BPaxos as well.
@JSExportAll
trait DependencyGraph {
  // See above. If an instance is committed after it has already been
  // committed, then `commit` ignores it and returns an empty sequence.
  def commit(
      instance: Instance,
      sequenceNumber: Int,
      dependencies: Set[Instance]
  ): Seq[Instance]
}
