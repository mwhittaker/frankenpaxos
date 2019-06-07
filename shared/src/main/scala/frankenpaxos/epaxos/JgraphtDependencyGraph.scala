package frankenpaxos.epaxos

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
    val executable: Seq[Instance] = iterator.asScala
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

    executable
  }
}
