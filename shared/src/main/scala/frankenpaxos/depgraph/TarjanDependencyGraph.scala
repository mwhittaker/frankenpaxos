package frankenpaxos.depgraph

import frankenpaxos.compact.CompactSet
import scala.collection.mutable
import scala.scalajs.js.annotation.JSExport
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
@JSExportAll
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
  // TODO(mwhittaker): Optimization opportunities for EPaxos/BPaxos-specific
  // dependency graph.
  //
  // - Use VertexIdBufferMap for `vertices`. Makes garbage collection faster.
  // - Attempt to execute vertices in increasing vertex id order.

  @JSExportAll
  case class Vertex(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: KeySet
  )

  @JSExportAll
  case class VertexMetadata(
      var number: Int,
      var lowLink: Int,
      var stackIndex: Int,
      var eligible: Boolean
  )

  // execute and executeByComponent are very similar. They differ only in how
  // they flatten components. Turns out this can be a big performance
  // difference, so we implement each with a custom way of appending executable
  // nodes. See below for more details.
  private trait Appender[A] {
    def appendOne(keys: mutable.Buffer[A], key: Key): Unit
    def appendMany(keys: mutable.Buffer[A], newKeys: mutable.Buffer[Key]): Unit
  }

  @JSExport
  protected val vertices = mutable.Map[Key, Vertex]()

  @JSExport
  protected val executed: KeySet = emptyKeySet

  // Metadata used by execute.
  private val metadatas = mutable.Map[Key, VertexMetadata]()
  private val stack = mutable.Buffer[Key]()
  private val executables = mutable.Buffer[Key]()
  private val blockers = mutable.Set[Key]()

  // Flattened appender, used by execute and appendExecute.
  private val flatAppender = new Appender[Key] {
    def appendOne(keys: mutable.Buffer[Key], key: Key): Unit = keys += key
    def appendMany(
        keys: mutable.Buffer[Key],
        newKeys: mutable.Buffer[Key]
    ): Unit = keys ++= newKeys
  }

  // Batched appender, used by executeByComponent.
  private val batchedAppender = new Appender[Seq[Key]] {
    def appendOne(keys: mutable.Buffer[Seq[Key]], key: Key): Unit =
      keys += Seq(key)

    def appendMany(
        keys: mutable.Buffer[Seq[Key]],
        newKeys: mutable.Buffer[Key]
    ): Unit = keys += newKeys.toSeq
  }

  override def commit(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: KeySet
  ): Unit = {
    // Ignore repeated commands.
    if (vertices.contains(key) || executed.contains(key)) {
      return
    }

    vertices(key) = Vertex(key, sequenceNumber, dependencies)
  }

  override def updateExecuted(keys: KeySet): Unit = {
    executed.addAll(keys)
    vertices.retain({ case (key, _) => !executed.contains(key) })
  }

  private def returnExecutables(executables: Seq[Seq[Key]]): Seq[Seq[Key]] = {
    for {
      component <- executables
      key <- component
    } {
      vertices -= key
      executed.add(key)
    }
    executables
  }

  override def execute(numBlockers: Option[Int]): (Seq[Key], Set[Key]) = {
    metadatas.clear()
    stack.clear()
    executables.clear()
    blockers.clear()

    executeImpl[Key](numBlockers, flatAppender, executables, blockers)
    val result = (executables.toSeq, blockers.toSet)

    for (key <- executables) {
      vertices -= key
      executed.add(key)
    }

    result
  }

  override def appendExecute(
      numBlockers: Option[Int],
      executables: mutable.Buffer[Key],
      blockers: mutable.Set[Key]
  ): Unit = {
    metadatas.clear()
    stack.clear()
    val startIndex = executables.size
    executeImpl[Key](numBlockers, flatAppender, executables, blockers)

    for (i <- startIndex until executables.size) {
      val key = executables(i)
      vertices -= key
      executed.add(key)
    }
  }

  override def executeByComponent(
      numBlockers: Option[Int]
  ): (Seq[Seq[Key]], Set[Key]) = {
    metadatas.clear()
    stack.clear()
    val executables = mutable.Buffer[Seq[Key]]()
    val blockers = mutable.Set[Key]()
    executeImpl[Seq[Key]](numBlockers, batchedAppender, executables, blockers)
    val result = (executables.toSeq, blockers.toSet)

    for {
      component <- executables
      key <- component
    } {
      vertices -= key
      executed.add(key)
    }

    result
  }

  private def executeImpl[A](
      numBlockers: Option[Int],
      appender: Appender[A],
      executables: mutable.Buffer[A],
      blockers: mutable.Set[Key]
  ): Unit = {
    for ((key, vertex) <- vertices) {
      if (!metadatas.contains(key)) {
        strongConnect(key, appender, executables, blockers)

        // If we encounter an ineligible vertex, the call stack returns
        // immediately, and all nodes along the way are marked ineligible, so
        // we clear the stack.
        if (!metadatas(key).eligible) {
          stack.clear()
        }

        // We may have a lot of blockers, but may not want to find all of them.
        // Once we have enough blockers, as specified by the blockers argument,
        // we return.
        numBlockers.foreach(n => {
          if (blockers.size >= n) {
            return
          }
        })
      }
    }
  }

  private def strongConnect[A](
      v: Key,
      appender: Appender[A],
      executables: mutable.Buffer[A],
      blockers: mutable.Set[Key]
  ): Unit = {
    val number = metadatas.size
    metadatas(v) = VertexMetadata(
      number = number,
      lowLink = number,
      stackIndex = stack.size,
      eligible = true
    )
    stack += v

    for (w <- vertices(v).dependencies.materializedDiff(executed)) {
      if (!vertices.contains(w)) {
        // If we depend on an uncommitted vertex, we are ineligible.
        // Immediately return and mark all nodes on the stack ineligible. The
        // stack will be cleared at the end of the unwinding. We also make sure
        // to record the fact that we are blocked on this vertex.
        metadatas(v).eligible = false
        blockers += w
        return
      } else if (!metadatas.contains(w)) {
        // If we haven't explored our dependency yet, we recurse.
        strongConnect(w, appender, executables, blockers)

        // If our child is ineligible, we are ineligible. We return
        // immediately.
        if (!metadatas(w).eligible) {
          metadatas(v).eligible = false
          return
        }

        metadatas(v).lowLink =
          Math.min(metadatas(v).lowLink, metadatas(w).lowLink)
        metadatas(v).eligible = metadatas(v).eligible && metadatas(w).eligible
      } else if (!metadatas(w).eligible) {
        // If we depend on an ineligible vertex, we are ineligible.
        // Immediately return and mark all nodes on the stack ineligible. The
        // stack will be cleared at the end of the unwinding.
        metadatas(v).eligible = false
        return
      } else if (metadatas(w).stackIndex != -1) {
        metadatas(v).lowLink =
          Math.min(metadatas(v).lowLink, metadatas(w).number)
        metadatas(v).eligible = metadatas(v).eligible && metadatas(w).eligible
      } else {
        metadatas(v).eligible = metadatas(v).eligible && metadatas(w).eligible
      }
    }

    // v is not the root of its strongly connected component.
    if (metadatas(v).lowLink != metadatas(v).number) {
      return
    }

    // v is the root of its strongly connected component. The nodes in the
    // component are v and all nodes after v in the stack. This is all the
    // nodes in the range v.stackIndex to the end of the stack.
    if (metadatas(v).stackIndex == stack.size - 1) {
      val component = stack.last
      stack.remove(stack.size - 1)
      metadatas(component).stackIndex = -1
      appender.appendOne(executables, component)
    } else {
      val component = stack.slice(metadatas(v).stackIndex, stack.size)
      stack.remove(metadatas(v).stackIndex,
                   stack.size - metadatas(v).stackIndex)
      for (w <- component) {
        metadatas(w).stackIndex = -1
      }
      appender.appendMany(
        executables,
        component.sortBy(k => (vertices(k).sequenceNumber, k))
      )
    }
  }

  override def numVertices: Int = vertices.size
}
