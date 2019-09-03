package frankenpaxos.depgraph

import frankenpaxos.compact.CompactSet
import scala.collection.mutable
import scala.scalajs.js.annotation.JSExportAll
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.mutable.Graph

// ScalaGraphDependencyGraph is a DependencyGraph implemented using scala-graph
// [1]. scala-graph is not a very complete library. It is missing a lot of
// useful methods. For example, you can traverse weakly connected components in
// topological order but not strongly connected components! This makes it hard
// to implement DependencyGraph, but because scala-graph is a scala library, we
// can use it in the JS visualizations.
//
// [1]: http://www.scala-graph.org/
@JSExportAll
class ScalaGraphDependencyGraph[
    Key,
    SequenceNumber,
    KeySet <: CompactSet[KeySet] { type T = Key }
](
    val emptyKeySet: KeySet
)(
    implicit override val keyOrdering: Ordering[Key],
    implicit override val sequenceNumberOrdering: Ordering[SequenceNumber]
) extends DependencyGraph[Key, SequenceNumber, KeySet] {
  // We implement ScalaGraphDependencyGraph the same way we implement
  // JgraphtDependencyGraph. See JgraphtDependencyGraph for documentation.
  private val graph = Graph[Key, DiEdge]()
  private val committed = mutable.Set[Key]()
  private val sequenceNumbers = mutable.Map[Key, SequenceNumber]()
  private val executed: KeySet = emptyKeySet

  override def toString(): String = graph.toString

  override def commit(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: KeySet
  ): Unit = {
    // Ignore commands that have already been committed.
    if (committed.contains(key) || executed.contains(key)) {
      return
    }

    // Update our bookkeeping.
    committed += key
    sequenceNumbers(key) = sequenceNumber

    // Update the graph. If a dependency has already been executed, we don't
    // add an edge to it.
    graph.add(key)
    for (dependency <- dependencies.diff(executed).materialize()) {
      graph.add(dependency)
      graph.add(key ~> dependency)
    }
  }

  override def updateExecuted(keys: KeySet): Unit = {
    executed.addAll(keys)
    committed.retain(!executed.contains(_))
    sequenceNumbers.retain({ case (key, _) => !executed.contains(key) })

    val verticesToRemove = mutable.Set[Key]()
    for (key <- graph.nodes) {
      if (executed.contains(key)) {
        verticesToRemove += key
      }
    }
    for (key <- verticesToRemove) {
      graph -= key
    }
  }

  private def isEligible(key: Key): Boolean = {
    committed.contains(key) &&
    graph.outerNodeTraverser(graph.get(key)).forall(committed.contains(_))
  }

  // Note that executeByComponent ignores numBlockers. It returns all blockers.
  override def executeByComponent(
      numBlockers: Option[Int]
  ): (Seq[Seq[Key]], Set[Key]) = {
    // Filter out all vertices that are not eligible.
    val eligibleGraph = graph.filter(isEligible)

    // Condense the graph.
    val components = eligibleGraph.strongComponentTraverser()
    val componentIndex: Map[Key, eligibleGraph.Component] = {
      for {
        component <- components
        node <- component.nodes
      } yield {
        node.toOuter -> component
      }
    }.toMap
    val condensation = Graph[eligibleGraph.Component, DiEdge]()
    components.foreach(condensation.add(_))
    for {
      component <- components
      node <- component.nodes
      successor <- node.diSuccessors
    } {
      condensation.add(componentIndex(node) ~> componentIndex(successor))
    }

    // Reverse the edges of the graph.
    val edges = condensation.edges.toSeq
    for (edge <- edges) {
      val src = edge.head.toOuter
      val dst = edge.tail.head.toOuter
      condensation.remove(edge.toOuter)
      condensation.add(dst ~> src)
    }

    // Iterate through the graph in topological order. topologicalSort returns
    // either a node that is part of a cycle (Left) or the topological order.
    // Condensations are acyclic, so we should always get Right.
    val executable: Seq[Seq[Key]] = condensation.topologicalSort match {
      case Left(node) =>
        throw new IllegalStateException(
          s"Condensation $condensation has a cycle."
        )
      case Right(topologicalOrder) =>
        topologicalOrder
          .map(component => {
            component.nodes
              .map(_.toOuter)
              .toSeq
              .sortBy(key => (sequenceNumbers(key), key))
          })
          .toSeq
    }

    for {
      component <- executable
      key <- component
    } {
      graph.remove(key)
      committed -= key
      sequenceNumbers -= key
      executed.add(key)
    }

    (executable,
     graph.nodes.filter(!committed.contains(_)).map(_.toOuter).toSet)
  }

  // Returns the current set of nodes. This method is really only useful for
  // the Javascript visualizations.
  def nodes: Set[Key] =
    graph.nodes.map(_.toOuter).toSet

  // Returns the current set of edges. This method is really only useful for
  // the Javascript visualizations.
  def edges: Set[(Key, Key)] =
    graph.edges.map(edge => (edge.head.toOuter, edge.tail.head.toOuter)).toSet

  override def numVertices: Int = nodes.size
}
