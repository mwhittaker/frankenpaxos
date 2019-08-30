package frankenpaxos.depgraph

import frankenpaxos.compact.CompactSet
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
class JgraphtDependencyGraph[
    Key,
    SequenceNumber,
    KeySet <: CompactSet[KeySet] { type T = Key }
](
    val emptyKeySet: KeySet
)(
    implicit override val keyOrdering: Ordering[Key],
    implicit override val sequenceNumberOrdering: Ordering[SequenceNumber]
) extends DependencyGraph[Key, SequenceNumber, KeySet] {
  // The underlying graph. When a strongly connected component is "executed"
  // (i.e., returned by the `commit` method), it is removed from the graph.
  // This keeps the graph small.
  //
  // Note that just because an key exists in the graph doesn't mean it is
  // committed. For example, imagine we start with an empty graph and commit
  // key A with a dependency on key B. We create vertices for both A and B and
  // draw an edge from A to B, even though B is not committed. See `committed`
  // for the set of committed keys.
  private val graph: SimpleDirectedGraph[Key, DefaultEdge] =
    new SimpleDirectedGraph(classOf[DefaultEdge])

  // The set of keys that have been committed but not yet executed.  Vertices
  // in `graph` that are not in `committed` have not yet been committed.
  private val committed = mutable.Set[Key]()

  // The sequence numbers of the keys in `committed`.
  private val sequenceNumbers = mutable.Map[Key, SequenceNumber]()

  // The keys that have already been executed and removed from the graph.
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
    graph.addVertex(key)
    for (dependency <- dependencies.diff(executed).materialize()) {
      graph.addVertex(dependency)
      graph.addEdge(key, dependency)
    }
  }

  override def updateExecuted(keys: KeySet): Unit = {
    executed.addAll(keys)
    committed.retain(!executed.contains(_))
    sequenceNumbers.retain({ case (key, _) => !executed.contains(key) })

    val verticesToRemove = mutable.Set[Key]()
    for (key <- graph.vertexSet().asScala) {
      if (executed.contains(key)) {
        verticesToRemove += key
      }
    }
    graph.removeAllVertices(verticesToRemove.asJava)
  }

  // Returns whether an key is eligible. An key is eligible if all commands
  // reachable from the key (including itself) are committed.
  //
  // If an key has dependencies to commands that have already been executed,
  // then the edges to those dependencies will be absent because executed
  // commands are pruned. This doesn't affect the correctness of isEligible
  // though.
  private def isEligible(key: Key): Boolean = {
    val iterator = new BreadthFirstIterator(graph, key)
    committed.contains(key) &&
    iterator.asScala.forall(committed.contains(_))
  }

  override def executeByComponent(): Seq[Seq[Key]] = {
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
    val executable: Seq[Seq[Key]] = iterator.asScala
      .map(component => {
        component.vertexSet.asScala.toSeq
          .sortBy(key => (sequenceNumbers(key), key))
      })
      .toSeq

    for {
      component <- executable
      key <- component
    } {
      graph.removeVertex(key)
      committed -= key
      sequenceNumbers -= key
      executed.add(key)
    }

    executable
  }

  override def numVertices: Int = graph.vertexSet().size
}
