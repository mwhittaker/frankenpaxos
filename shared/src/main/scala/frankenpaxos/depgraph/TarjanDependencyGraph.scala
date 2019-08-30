package frankenpaxos.depgraph

import frankenpaxos.compact.CompactSet
import scala.collection.mutable
import scala.scalajs.js.annotation.JSExportAll

// TarjanDependencyGraph implements a dependency graph using Tarjans's strongly
// connected components algorithm [1, 2]. JgraphtDependencyGraph and
// ScalaGraphDependencyGraph both use existing graph libraries, but benchmarks
// show they are too slow. This leads to the dependency graph becoming a
// bottleneck in protocols. We implement Tarjan's algorithm ourselves to make
// the dependency graph run faster. Existing EPaxos implementations do this as
// well [3, 4].
//
// Tarjan's algorithm speeds things up for two reasons. First, unlike other
// strongly connected components algorithms, Tarjan's algorithm only needs one
// pass over the graph instead of two. Second, unlike other algorithms,
// Tarjan's algorithm outputs the components in reverse topological order. This
// is perfect for a dependency graph, since we want to output components in
// reverse topological order anyway. This avoids having to perform a separate
// topological sort.
//
// Logically, we construct a graph of all commands and their dependencies,
// committed or not. We prune the graph to include only eligible vertices, and
// then run Tarjan's algorithm to extract the components in reverse topological
// order, ignoring those we've already returned. We perform a couple of
// optimizations to make things faster:
//
//   1. Once a component has been returned, we can remove it from the graph.
//   2. We do not have vertices for uncommitted commands.
//   3. We determine whether nodes are eligible while running Tarjan's
//      algorithm.
//
// Optimization 1 and 2 are trivial. Optimization 3 is quite complicated. The
// main idea is as follows. Imagine taking the dependency graph and computing
// its condensation. Now, we have a directed acyclic graph. Moreover, every
// uncommitted command forms its own singleton connected component (because an
// uncommitted command has no outbound edges), so every uncommitted command is
// a leaf of the condensation. We can compute eligibility using a DFS on the
// condensation as follows:
//
//   def compute_eligibility():
//     for V in condensation:
//       if not V.explored:
//         DFS(V)
//
//   def DFS(V):
//     if V is uncommitted:
//       V.eligible = false
//     else
//       V.eligible = true
//       for dependency W of V:
//         if !W.explored:
//           DFS(W)
//         V.eligible = V.eligible and W.eligible
//       V.explored = true
//
// compute_eligibility is greatly simplified by the fact that there are no
// cycles. We can induct through the graph in reverse topological order to see
// that it is correct.
//
// Optimization 3 simply interlaces this algorithm with Tarjan's algorithm,
// taking advantage of the fact that Tarjan's algorithm implicitly visits
// components in reverse topological order.
//
// Tarjan's algorithm is hard to understand, various sites online describe the
// algorithm differently, and not many provide proofs of correctness. Our
// implementation of Tarjan's algorithm could be incorrect. And our
// optimization 3 could be incorrect.
//
// TODO(mwhittaker): Think of a more rigorous proof of correctness.
//
// [1]: https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
// [2]: https://scholar.google.com/scholar?cluster=15533190727229683002
// [3]: https://github.com/nvanbenschoten/epaxos/blob/master/epaxos/execute.go
// [4]: https://github.com/efficient/epaxos/blob/master/src/epaxos/epaxos-exec.go
class TarjanDependencyGraph[
    Key,
    SequenceNumber,
    KeySet <: CompactSet[KeySet] { type T = Key }
](
    // TODO(mwhittaker): Take in a factory instead.
    emptyKeySet: KeySet
)(
    implicit override val keyOrdering: Ordering[Key],
    implicit override val sequenceNumberOrdering: Ordering[SequenceNumber]
) extends DependencyGraph[Key, SequenceNumber, KeySet] {
  case class Vertex(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: Set[Key]
  )

  case class VertexMetadata(
      number: Int,
      lowLink: Int,
      onStack: Boolean,
      eligible: Boolean
  )

  val vertices = mutable.Map[Key, Vertex]()
  val executed: KeySet = emptyKeySet

  override def commit(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: KeySet
  ): Unit = {
    // Ignore repeated commands.
    if (vertices.contains(key) || executed.contains(key)) {
      return
    }

    vertices(key) =
      Vertex(key, sequenceNumber, dependencies.diff(executed).materialize())
  }

  override def updateExecuted(keys: KeySet): Unit = {
    executed.addAll(keys)
    vertices.retain({ case (key, _) => !executed.contains(key) })
  }

  override def executeByComponent(): Seq[Seq[Key]] = {
    val metadatas = mutable.Map[Key, VertexMetadata]()
    val stack = mutable.Buffer[Key]()
    val executables = mutable.Buffer[Seq[Key]]()

    for ((key, vertex) <- vertices) {
      if (!metadatas.contains(key)) {
        strongConnect(metadatas, stack, executables, key)
      }
    }

    // Remove the executed commands.
    for {
      component <- executables
      key <- component
    } {
      vertices -= key
      executed.add(key)
    }

    executables.toSeq
  }

  def strongConnect(
      metadatas: mutable.Map[Key, VertexMetadata],
      stack: mutable.Buffer[Key],
      executables: mutable.Buffer[Seq[Key]],
      v: Key
  ): Unit = {
    val number = metadatas.size
    metadatas(v) = VertexMetadata(
      number = number,
      lowLink = number,
      onStack = true,
      eligible = true
    )
    stack += v

    for (w <- vertices(v).dependencies if !executed.contains(w)) {
      if (!vertices.contains(w)) {
        // If we depend on an uncommitted vertex, we are ineligible.
        metadatas(v) = metadatas(v).copy(eligible = false)
      } else if (!metadatas.contains(w)) {
        // If we haven't explored our dependency yet, we recurse.
        strongConnect(metadatas, stack, executables, w)
        metadatas(v) = metadatas(v).copy(
          lowLink = Math.min(metadatas(v).lowLink, metadatas(w).lowLink),
          eligible = metadatas(v).eligible && metadatas(w).eligible
        )
      } else if (metadatas(w).onStack) {
        metadatas(v) = metadatas(v).copy(
          lowLink = Math.min(metadatas(v).lowLink, metadatas(w).number),
          eligible = metadatas(v).eligible && metadatas(w).eligible
        )
      } else {
        metadatas(v) = metadatas(v).copy(
          eligible = metadatas(v).eligible && metadatas(w).eligible
        )
      }
    }

    // v is not the root of its strongly connected component.
    if (metadatas(v).lowLink != metadatas(v).number) {
      return
    }

    // v is the root of its strongly connected component.
    val component = mutable.Buffer[Key]()

    // Pop all other nodes in our component
    while (stack.last != v) {
      val w = stack.last
      stack.remove(stack.size - 1)
      component += w
      metadatas(w) = metadatas(w).copy(onStack = false)
    }

    // Pop v.
    // TODO(mwhittaker): We might be able to store the stack index in the
    // VertexMetadata. Then, instead of continuously popping off the stack, we
    // can just slice the stack. This might make things go faster.
    component += stack.last
    stack.remove(stack.size - 1)
    metadatas(v) = metadatas(v).copy(onStack = false)

    // Check to see if the component is eligible. If it is, sort and execute
    // it. If not, mark all as ineligible.
    val eligible = component.forall(metadatas(_).eligible)
    if (!eligible) {
      component.foreach(k => metadatas(k) = metadatas(k).copy(eligible = false))
    } else {
      // Sort the component and append to executables.
      executables += component.sortBy(k => (vertices(k).sequenceNumber, k))
    }
  }

  override def numVertices: Int = vertices.size
}
