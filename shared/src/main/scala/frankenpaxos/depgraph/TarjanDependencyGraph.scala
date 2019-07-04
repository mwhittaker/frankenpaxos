package frankenpaxos.depgraph

import scala.collection.mutable
import scala.scalajs.js.annotation.JSExportAll

class TarjanDependencyGraph[Key, SequenceNumber]()(
    implicit override val keyOrdering: Ordering[Key],
    implicit override val sequenceNumberOrdering: Ordering[SequenceNumber]
) extends DependencyGraph[Key, SequenceNumber] {

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
  val executed = mutable.Set[Key]()

  override def commit(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: Set[Key]
  ): Unit = {
    // Ignore repeated commands.
    if (vertices.contains(key) || executed.contains(key)) {
      return
    }

    vertices(key) = Vertex(key, sequenceNumber, dependencies)
  }

  override def execute(): Seq[Key] = {
    val metadatas = mutable.Map[Key, VertexMetadata]()
    val stack = mutable.Buffer[Key]()
    val executables = mutable.Buffer[Key]()

    for ((key, vertex) <- vertices) {
      if (!metadatas.contains(key)) {
        strongConnect(metadatas, stack, executables, key)
      }
    }

    // Remove the executed commands.
    for (k <- executables) {
      vertices -= k
      executed += k
    }
    executables.toSeq
  }

  def strongConnect(
      metadatas: mutable.Map[Key, VertexMetadata],
      stack: mutable.Buffer[Key],
      executables: mutable.Buffer[Key],
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
          // TODO(mwhittaker): Is this correct?
          eligible = metadatas(v).eligible && metadatas(w).eligible
        )
      } else {
        metadatas(v) = metadatas(v).copy(
          // TODO(mwhittaker): Is this correct?
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
      executables ++= component.sortBy(k => (vertices(k).sequenceNumber, k))
    }
  }

  override def numNodes: Int = vertices.size

  override def numEdges: Int = {
    // TODO(mwhittaker): Implement
    ???
  }
}
