package frankenpaxos.epaxos

import java.util.Collections
import java.util.Comparator
import org.jgrapht
import org.jgrapht.Graph
import org.jgrapht.alg.KosarajuStrongConnectivityInspector
import org.jgrapht.alg.interfaces.StrongConnectivityAlgorithm
import org.jgrapht.graph.AsSubgraph
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.EdgeReversedGraph
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.BreadthFirstIterator
import org.jgrapht.traverse.TopologicalOrderIterator
import scala.collection.JavaConverters._
import scala.collection.mutable
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

// JgraphtDependencyGraph is a DependencyGraph implemented using JGraphT [1].
// JGraphT is a feature rich library that makes it easy to implement
// DependencyGraph. However, it is a Java library, so it cannot be used in the
// JS visualizations.
//
// [1]: https://jgrapht.org/
class JgraphtDependencyGraph extends DependencyGraph {
  // The underlying graph. When a strongly connected component is "executed"
  // (i.e., returned by the `commit` method), it is removed from the graph.
  // This keeps the graph small.
  //
  // Note that just because an instance exists in the graph doesn't mean it is
  // committed. For example, imagine we start with an empty graph and commit
  // instance A with a dependency on instance B. We create vertices for both A
  // and B and draw an edge from A to B, even though B is not committed. See
  // `committed` for the set of committed instances.
  private val graph: SimpleDirectedGraph[Instance, DefaultEdge] =
    new SimpleDirectedGraph(classOf[DefaultEdge])

  // The set of instances that have been committed but not yet executed.
  // Vertices in `graph` that are not in `committed` have not yet been
  // committed.
  private val committed = mutable.Set[Instance]()

  // The sequence numbers of the instances in `committed`.
  private val sequenceNumbers = mutable.Map[Instance, Int]()

  // The instances that have already been executed and removed from the graph.
  private val executed = mutable.Set[Instance]()

  override def toString(): String = graph.toString

  override def commit(
      instance: Instance,
      sequenceNumber: Int,
      dependencies: Set[Instance]
  ): Seq[Instance] = {
    // Ignore commands that have already been committed.
    if (committed.contains(instance) || executed.contains(instance)) {
      return Seq()
    }

    // Update our bookkeeping.
    committed += instance
    sequenceNumbers(instance) = sequenceNumber

    // Update the graph.
    graph.addVertex(instance)
    for (dependency <- dependencies) {
      // If a dependency has already been executed, we don't add an edge to it.
      if (!executed.contains(dependency)) {
        graph.addVertex(dependency)
        graph.addEdge(instance, dependency)
      }
    }

    // Execute the graph.
    execute()
  }

  // Returns whether an instance is eligible. An instance is eligible if all
  // commands reachable from the instance (including itself) are committed.
  //
  // If an instance has dependencies to commands that have already been
  // executed, then the edges to those dependencies will be absent because
  // executed commands are pruned. This doesn't affect the correctness of
  // isEligible though.
  private def isEligible(instance: Instance): Boolean = {
    val iterator = new BreadthFirstIterator(graph, instance)
    committed.contains(instance) &&
    iterator.asScala.forall(committed.contains(_))
  }

  // Try to execute as much of the graph as possible.
  private def execute(): Seq[Instance] = {
    // 1. Filter out all vertices that are not eligible.
    // 2. Condense the graph.
    // 3. Execute the graph in reverse topological order, sorting by sequence
    //    number within a component.
    val eligible = graph.vertexSet().asScala.filter(isEligible)
    val eligibleGraph = new AsSubgraph(graph, eligible.asJava)
    val components = new KosarajuStrongConnectivityInspector(eligibleGraph)
    val condensation = components.getCondensation()
    val reversed = new EdgeReversedGraph(condensation)
    val iterator = new TopologicalOrderIterator(reversed)
    val executable = iterator.asScala
      .flatMap(component => {
        component.vertexSet.asScala.toSeq
          .sortBy({
            case instance @ Instance(replicaIndex, instanceNumber) =>
              (sequenceNumbers(instance), replicaIndex, instanceNumber)
          })
      })
      .toSeq

    for (instance <- executable) {
      graph.removeVertex(instance)
      committed -= instance
      sequenceNumbers -= instance
      executed += instance
    }

    executable.toSeq
  }
}

// ScalaGraphDependencyGraph is a DependencyGraph implemented using scala-graph
// [1]. scala-graph is not a very complete library. It is missing a lot of
// useful methods. For example, you can traverse weakly connected components in
// topological order but not strongly connected components! This makes it hard
// to implement DependencyGraph, but because scala-graph is a scala library, we
// can use it in the JS visualizations.
//
// [1]: http://www.scala-graph.org/
class ScalaGraphDependencyGraph extends DependencyGraph {
  override def commit(
      instance: Instance,
      sequenceNumber: Int,
      dependencies: Set[Instance]
  ): Seq[Instance] = {
    // TODO(mwhittaker): Implement.
    ???
  }
}
