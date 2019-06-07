package frankenpaxos.epaxos

import scala.collection.mutable
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
class ScalaGraphDependencyGraph extends DependencyGraph {
  // We implement ScalaGraphDependencyGraph the same way we implement
  // JgraphtDependencyGraph. See JgraphtDependencyGraph for documentation.
  private val graph = Graph[Instance, DiEdge]()
  private val committed = mutable.Set[Instance]()
  private val sequenceNumbers = mutable.Map[Instance, Int]()
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
    graph.add(instance)
    for (dependency <- dependencies) {
      // If a dependency has already been executed, we don't add an edge to it.
      if (!executed.contains(dependency)) {
        graph.add(dependency)
        graph.add(instance ~> dependency)
      }
    }

    // Execute the graph.
    execute()
  }

  private def isEligible(instance: Instance): Boolean = {
    committed.contains(instance) &&
    graph.outerNodeTraverser(graph.get(instance)).forall(committed.contains(_))
  }

  private def execute(): Seq[Instance] = {
    // Filter out all vertices that are not eligible.
    val eligibleGraph = graph.filter(isEligible)

    // Condense the graph.
    val components = eligibleGraph.strongComponentTraverser()
    val componentIndex: Map[Instance, eligibleGraph.Component] = {
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
    val executable: Seq[Instance] = condensation.topologicalSort match {
      case Left(node) =>
        throw new IllegalStateException(
          s"Condensation $condensation has a cycle."
        )
      case Right(topologicalOrder) =>
        topologicalOrder
          .flatMap(component => {
            component.nodes
              .map(_.toOuter)
              .toSeq
              .sortBy({
                case instance @ Instance(replicaIndex, instanceNumber) =>
                  (sequenceNumbers(instance), replicaIndex, instanceNumber)
              })
          })
          .toSeq
    }

    for (instance <- executable) {
      graph.remove(instance)
      committed -= instance
      sequenceNumbers -= instance
      executed += instance
    }

    executable
  }
}
