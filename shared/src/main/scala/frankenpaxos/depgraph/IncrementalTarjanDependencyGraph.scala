package frankenpaxos.depgraph

import frankenpaxos.compact.CompactSet
import scala.collection.mutable
import scala.scalajs.js.annotation.JSExportAll
import scala.util.control.Breaks._

// IncrementalTarjanDependencyGraph implements a dependency graph using an
// incremental variant of Tarjans's strongly connected components algorithm [1,
// 2].
//
// JgraphtDependencyGraph and ScalaGraphDependencyGraph both use existing graph
// libraries, but benchmarks show they are too slow. This leads to the
// dependency graph becoming a bottleneck in protocols. TarjanDependencyGraph
// implements Tarjan's algorithm from scratch, specializing it to our
// particular application. This allows TarjanDependencyGraph to outperform
// JgraphtDependencyGraph and ScalaGraphDependencyGraph. However,
// TarjanDependencyGraph executes Tarjan's algorithm from scratch every time
// execute() is invoked. This can lead to lots of redundant work.
//
// IncrementalTarjanDependencyGraph implements an incremental variant of
// Tarjan's algorithm. It doesn't perform any unnecessary work, but it can
// somtimes delay the execution of eligible commands. Thus,
// IncrementalTarjanDependencyGraph is not strictly better or strictly worse
// than TarjanDependencyGraph.
//
// [1]: https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
// [2]: https://scholar.google.com/scholar?cluster=15533190727229683002
class IncrementalTarjanDependencyGraph[
    Key,
    SequenceNumber,
    KeySet <: CompactSet[KeySet] { type T = Key }
](
    val emptyKeySet: KeySet
)(
    implicit override val keyOrdering: Ordering[Key],
    implicit override val sequenceNumberOrdering: Ordering[SequenceNumber]
) extends DependencyGraph[Key, SequenceNumber, KeySet] {
  // The result of calling the `strongConnect` method. When we call
  // strongConnect, if Tarjan's algorithm encounters an uncommitted vertex, it
  // must pause and resume later. Otherwise, if no uncommitted vertices are
  // hit, the algorithm runs successfully.
  sealed trait Result
  case object Paused extends Result
  case object Success extends Result

  // A vertex in the dependency graph.
  case class Vertex(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: Seq[Key]
  )

  // The metadata needed for Tarjan's algorithm.
  case class VertexMetadata(
      number: Int,
      lowLink: Int,
      onStack: Boolean,
      // The index into this vertex' dependencies of the dependency this vertex
      // is currently processing. If all dependencies of a vertex are
      // committed, then this field is not really used. If a vertex is being
      // processed and hits an uncommitted dependency, then the algorithm is
      // paused and currentDependency indexes the uncommitted vertex. When the
      // algorithm resumes, currentDependency is used to start the algorithm
      // from where it paused.
      currentDependency: Int
  )

  // The set of vertices in the dependency graph.
  val vertices = mutable.Map[Key, Vertex]()

  // The set of executed vertices.
  val executed: KeySet = emptyKeySet

  // callstack simulates a recursive implementation of strongConnect. We have
  // to manually manage our own stack because we pause the algorithm and resume
  // it later.
  val callstack = mutable.Buffer[Key]()

  // The state of Tarjan's algorithm.
  val metadatas = mutable.Map[Key, VertexMetadata]()
  val stack = mutable.Buffer[Key]()
  val executables = mutable.Buffer[Seq[Key]]()

  override def commit(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: KeySet
  ): Unit = {
    // Ignore repeated commands.
    if (vertices.contains(key) || executed.contains(key)) {
      return
    }

    // We pre-process the dependencies in two ways. First, we remove any
    // dependencies on executed commands. Once a command has been executed,
    // there is no need to depend on it. Second, we place dependencies on
    // committed commands before dependencies on uncommitted commands. During
    // Tarjan's algorithm, when we encounter an uncommitted command, we pause
    // the algorithm. Thus, placing dependencies on committed commands first
    // helps the algorithm run as far along as possible.
    val (committed, uncommitted) = dependencies
      .diff(executed)
      .materialize()
      .toSeq
      .partition(vertices.contains(_))
    vertices(key) = Vertex(key, sequenceNumber, committed ++ uncommitted)
  }

  override def executeByComponent(): Seq[Seq[Key]] = {
    if (!callstack.isEmpty) {
      strongConnect() match {
        case Paused  => return collectExecutables()
        case Success => // Keep going.
      }
    }

    for ((key, _) <- vertices) {
      if (!metadatas.contains(key)) {
        callstack += key
        strongConnect() match {
          case Paused  => return collectExecutables()
          case Success => // Keep going.
        }
      }
    }

    // We've finished a pass of the algorithm. Now, it's safe to clear our
    // metadata and start fresh in the next invocation of execute.
    assert(callstack.isEmpty)
    metadatas.clear()
    assert(stack.isEmpty)
    collectExecutables()
  }

  // Return and prune the executable vertices.
  private def collectExecutables(): Seq[Seq[Key]] = {
    for {
      component <- executables
      key <- component
    } {
      vertices -= key
      executed.add(key)
    }
    // Note that toSeq is not correct here. Clearing executables clears the
    // toSeq.
    val seq = Seq[Seq[Key]]() ++ executables
    executables.clear()
    return seq
  }

  def strongConnect(): Result = {
    while (!callstack.isEmpty) {
      val v = callstack.last
      metadatas.get(v) match {
        case Some(_) =>
        // We've already begun processing this vertex.

        case None =>
          // We've never seen this vertex before.
          metadatas(v) = VertexMetadata(
            number = metadatas.size,
            lowLink = metadatas.size,
            onStack = true,
            currentDependency = 0
          )
          stack += v
      }

      val dependencies = vertices(v).dependencies
      breakable {
        for (i <- metadatas(v).currentDependency until dependencies.size) {
          val w = dependencies(i)
          if (executed.contains(w)) {
            // If w has already been executed, skip it.
          } else if (!vertices.contains(w)) {
            // If we depend on an uncommitted vertex, we have to pause until it
            // is ready.
            return Paused
          } else if (!metadatas.contains(w)) {
            // If we haven't explored our dependency yet, we "recurse".
            callstack += w
            break
            // If we recursed normally here, we'd have the code below. Since
            // we're not actually recursing, we instead move the code down way
            // below.
            //
            // metadatas(v) = metadatas(v).copy(
            //   lowLink = Math.min(metadatas(v).lowLink, metadatas(w).lowLink),
            // )
          } else if (metadatas(w).onStack) {
            metadatas(v) = metadatas(v).copy(
              lowLink = Math.min(metadatas(v).lowLink, metadatas(w).number)
            )
          }

          metadatas(v) = metadatas(v).copy(currentDependency = i + 1)
        }
      }

      if (metadatas(v).currentDependency != dependencies.size) {
        // We didn't finish processing the loop above. We have to "recurse". To
        // "recurse". We simply do nothing. The next iteration of the loop will
        // pick up the new element on the stack.
      } else {
        // We did finish the loop above! We form any strongly connected
        // components possible and then unwind the stack.

        // v is the root of its strongly connected component.
        if (metadatas(v).lowLink == metadatas(v).number) {
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
          // VertexMetadata. Then, instead of continuously popping off the
          // stack, we can just slice the stack. This might make things go
          // faster.
          component += stack.last
          stack.remove(stack.size - 1)
          metadatas(v) = metadatas(v).copy(onStack = false)
          executables += component.sortBy(k => (vertices(k).sequenceNumber, k))
        }

        // This code would normally be in the loop higher up, but since we're
        // not actually recursing, we move it down here.
        callstack.remove(callstack.size - 1)
        if (!callstack.isEmpty) {
          metadatas(callstack.last) = metadatas(callstack.last).copy(
            lowLink =
              Math.min(metadatas(callstack.last).lowLink, metadatas(v).lowLink)
          )
        }
      }
    }

    Success
  }

  override def numVertices: Int = vertices.size
}
