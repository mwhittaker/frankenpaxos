package frankenpaxos.depgraph

import frankenpaxos.compact.CompactSet
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.util.BufferMap
import frankenpaxos.util.VertexIdLike
import scala.collection.mutable
import scala.scalajs.js.annotation.JSExport
import scala.scalajs.js.annotation.JSExportAll

case class ZigzagTarjanDependencyGraphOptions(
    verticesGrowSize: Int,
    garbageCollectEveryNCommands: Int
)

object ZigzagTarjanDependencyGraphOptions {
  val default = ZigzagTarjanDependencyGraphOptions(
    verticesGrowSize = 1000,
    garbageCollectEveryNCommands = 1000
  )
}

@JSExportAll
class ZigzagTarjanDependencyGraphMetrics(collectors: Collectors) {
  val methodTotal: Counter = collectors.counter
    .build()
    .name("zigzag_tarjan_method_total")
    .labelNames("name")
    .help("Total number of method invocations.")
    .register()

  val ignoredTotal: Counter = collectors.counter
    .build()
    .name("zigzag_tarjan_ignored_total")
    .help(
      "Total number of committed vertices that were ignored because they " +
        "were already committed or already executed."
    )
    .register()

  val alreadyExecutedTotal: Counter = collectors.counter
    .build()
    .name("zigzag_tarjan_already_executed_total")
    .help("Total number of vertices in executeImpl that were already executed.")
    .register()

  val inMetadatasTotal: Counter = collectors.counter
    .build()
    .name("zigzag_tarjan_in_metadatas_total")
    .help("Total number of vertices in executeImpl that are in metadatas.")
    .register()

  val notInMetadatasTotal: Counter = collectors.counter
    .build()
    .name("zigzag_tarjan_not_in_metadatas_total")
    .help("Total number of vertices in executeImpl that are not in metadatas.")
    .register()

  val ineligibleStackSize: Summary = collectors.summary
    .build()
    .name("zigzag_tarjan_ineligible_stack_size")
    .help("Size of stack upon finding an ineligible vertex.")
    .register()

  val earlyBlockersReturnTotal: Counter = collectors.counter
    .build()
    .name("zigzag_tarjan_early_blockers_return_total")
    .help(
      "Total number of times execution is stopped because enough blockers " +
        "are found."
    )
    .register()

  val numChildrenVisited: Summary = collectors.summary
    .build()
    .name("zigzag_tarjan_num_children_visited")
    .help("The number of children a node visits in strongConnect.")
    .register()

  val strongConnectBranchTotal: Counter = collectors.counter
    .build()
    .name("zigzag_tarjan_strong_connect_branch_total")
    .labelNames("branch")
    .help("Total number of times each branch of strongConnect is taken.")
    .register()

  val componentSize: Summary = collectors.summary
    .build()
    .name("zigzag_tarjan_component_size")
    .help("The size of a strongly connected component.")
    .register()
}

object ZigzagTarjanDependencyGraph {
  trait Appender[A, Key] {
    def appendOne(keys: mutable.Buffer[A], key: Key): Unit
    def appendMany(keys: mutable.Buffer[A], newKeys: mutable.Buffer[Key]): Unit
  }

  class FlatAppender[Key] extends Appender[Key, Key] {
    def appendOne(keys: mutable.Buffer[Key], key: Key): Unit = keys += key
    def appendMany(
        keys: mutable.Buffer[Key],
        newKeys: mutable.Buffer[Key]
    ): Unit = keys ++= newKeys
  }

  class BatchedAppender[Key] extends Appender[Seq[Key], Key] {
    def appendOne(keys: mutable.Buffer[Seq[Key]], key: Key): Unit =
      keys += Seq(key)

    def appendMany(
        keys: mutable.Buffer[Seq[Key]],
        newKeys: mutable.Buffer[Key]
    ): Unit = keys += newKeys.toSeq
  }
}

@JSExportAll
class ZigzagTarjanDependencyGraph[
    Key,
    SequenceNumber,
    KeySet <: CompactSet[KeySet] { type T = Key }
](
    // TODO(mwhittaker): Take in a factory instead.
    emptyKeySet: KeySet,
    numLeaders: Int,
    like: VertexIdLike[Key],
    options: ZigzagTarjanDependencyGraphOptions =
      ZigzagTarjanDependencyGraphOptions.default,
    metrics: ZigzagTarjanDependencyGraphMetrics =
      new ZigzagTarjanDependencyGraphMetrics(
        PrometheusCollectors
      )
)(
    implicit override val keyOrdering: Ordering[Key],
    implicit override val sequenceNumberOrdering: Ordering[SequenceNumber]
) extends DependencyGraph[Key, SequenceNumber, KeySet] {
  // TODO(mwhittaker): Optimization opportunities for EPaxos/BPaxos-specific
  // dependency graph.
  //
  // - Use VertexIdBufferMap for `vertices`. Makes garbage collection faster.
  // - Attempt to execute vertices in increasing vertex id order.
  import ZigzagTarjanDependencyGraph._

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

  @JSExport
  protected val vertices = mutable.Buffer.fill[BufferMap[Vertex]](numLeaders)(
    new BufferMap(options.verticesGrowSize)
  )

  @JSExport
  protected var executedWatermark = mutable.Buffer.fill[Int](numLeaders)(0)

  @JSExport
  protected var numCommandsSinceLastGc: Int = 0

  @JSExport
  protected val executed: KeySet = emptyKeySet

  // Appenders.
  private val flatAppender = new FlatAppender[Key]()
  private val batchedAppender = new BatchedAppender[Key]()

  // Metadata used by execute.
  private val metadatas = mutable.Map[Key, VertexMetadata]()
  private val stack = mutable.Buffer[Key]()
  private val executables = mutable.Buffer[Key]()
  private val blockers = mutable.Set[Key]()

  // Helpers ///////////////////////////////////////////////////////////////////
  private def getVertex(key: Key): Option[Vertex] =
    vertices(like.leaderIndex(key)).get(like.id(key))

  private def containsVertex(key: Key): Boolean =
    getVertex(key).isDefined

  private def next(leaderIndex: Int): (Key, Option[Vertex]) = {
    val id = executedWatermark(leaderIndex)
    (like.make(leaderIndex, id), vertices(leaderIndex).get(id))
  }

  private def garbageCollect(): Unit = {
    for (i <- 0 until numLeaders) {
      vertices(i).garbageCollect(executedWatermark(i))
    }
  }

  // Methods ///////////////////////////////////////////////////////////////////
  // Unfortunately, it's not easy to get the size of a buffer map, so we just
  // report 42 here.
  override def numVertices: Int = 42

  override def commit(
      key: Key,
      sequenceNumber: SequenceNumber,
      dependencies: KeySet
  ): Unit = {
    metrics.methodTotal.labels("commit").inc()

    // Ignore repeated commands.
    if (vertices.contains(key) || executed.contains(key)) {
      metrics.ignoredTotal.inc()
      return
    }

    vertices(like.leaderIndex(key))
      .put(like.id(key), Vertex(key, sequenceNumber, dependencies))
  }

  override def updateExecuted(keys: KeySet): Unit = executed.addAll(keys)

  override def execute(numBlockers: Option[Int]): (Seq[Key], Set[Key]) = {
    metrics.methodTotal.labels("execute").inc()

    metadatas.clear()
    stack.clear()
    executables.clear()
    blockers.clear()

    executeImpl[Key](flatAppender, executables, blockers)
    val result = (executables.toSeq, blockers.toSet)

    numCommandsSinceLastGc += executables.size
    if (numCommandsSinceLastGc >= options.garbageCollectEveryNCommands) {
      garbageCollect()
      numCommandsSinceLastGc = 0
    }

    result
  }

  override def appendExecute(
      numBlockers: Option[Int],
      executables: mutable.Buffer[Key],
      blockers: mutable.Set[Key]
  ): Unit = {
    metrics.methodTotal.labels("appendExecute").inc()

    metadatas.clear()
    stack.clear()
    val startIndex = executables.size
    executeImpl[Key](flatAppender, executables, blockers)

    numCommandsSinceLastGc += (executables.size - startIndex)
    if (numCommandsSinceLastGc >= options.garbageCollectEveryNCommands) {
      garbageCollect()
      numCommandsSinceLastGc = 0
    }
  }

  override def executeByComponent(
      numBlockers: Option[Int]
  ): (Seq[Seq[Key]], Set[Key]) = {
    metrics.methodTotal.labels("executeByComponent").inc()

    metadatas.clear()
    stack.clear()
    val executables = mutable.Buffer[Seq[Key]]()
    val blockers = mutable.Set[Key]()
    executeImpl[Seq[Key]](batchedAppender, executables, blockers)
    val result = (executables.toSeq, blockers.toSet)

    for { component <- executables; key <- component } {
      numCommandsSinceLastGc += 1
    }
    if (numCommandsSinceLastGc >= options.garbageCollectEveryNCommands) {
      garbageCollect()
      numCommandsSinceLastGc = 0
    }

    result
  }

  // Implementation ////////////////////////////////////////////////////////////
  private def executeImpl[A](
      appender: Appender[A, Key],
      executables: mutable.Buffer[A],
      blockers: mutable.Set[Key]
  ): Unit = {
    metrics.methodTotal.labels("executeImpl").inc()
    for (i <- 0 until numLeaders) {
      executeColumnImpl(appender, executables, blockers, i)
    }
  }

  private def executeColumnImpl[A](
      appender: Appender[A, Key],
      executables: mutable.Buffer[A],
      blockers: mutable.Set[Key],
      leaderIndex: Int
  ): Unit = {
    metrics.methodTotal.labels("executeColumnImpl").inc()

    while (true) {
      val id = executedWatermark(leaderIndex)
      val key = like.make(leaderIndex, id)
      vertices(leaderIndex).get(id) match {
        case None =>
          blockers += key
          return

        case Some(vertex) =>
          if (executed.contains(key)) {
            // We've already executed this vertex in a previous invocation of
            // executeImpl.
            metrics.alreadyExecutedTotal.inc()
          } else if (!metadatas.contains(key)) {
            // This vertex hasn't been executed yet and this is the first time
            // we're visiting it on this invocation of executeImpl.
            metrics.notInMetadatasTotal.inc()
            strongConnect(key, appender, executables, blockers)

            // If we encounter an ineligible vertex, the call stack returns
            // immediately, and all nodes along the way are marked ineligible,
            // so we clear the stack.
            if (!metadatas(key).eligible) {
              metrics.ineligibleStackSize.observe(stack.size)
              stack.clear()
              metrics.earlyBlockersReturnTotal.inc()
              return
            }
          } else {
            // This vertex has been executed on this invocation of executeImpl
            // and was executed.
            metrics.inMetadatasTotal.inc()
          }

          executedWatermark(leaderIndex) += 1
      }
    }
  }

  private def strongConnect[A](
      v: Key,
      appender: Appender[A, Key],
      executables: mutable.Buffer[A],
      blockers: mutable.Set[Key]
  ): Unit = {
    metrics.methodTotal.labels("strongConnect").inc()

    val number = metadatas.size
    metadatas(v) = VertexMetadata(
      number = number,
      lowLink = number,
      stackIndex = stack.size,
      eligible = true
    )
    stack += v

    var numChildren = 0
    for (w <- getVertex(v).get.dependencies.materializedDiff(executed)) {
      numChildren += 1

      if (!containsVertex(w)) {
        // If we depend on an uncommitted vertex, we are ineligible.
        // Immediately return and mark all nodes on the stack ineligible. The
        // stack will be cleared at the end of the unwinding. We also make sure
        // to record the fact that we are blocked on this vertex.
        metrics.strongConnectBranchTotal.labels("uncommitted child").inc()
        metadatas(v).eligible = false
        blockers += w
        metrics.numChildrenVisited.observe(numChildren)
        return
      } else if (!metadatas.contains(w)) {
        // If we haven't explored our dependency yet, we recurse.
        metrics.strongConnectBranchTotal.labels("unexplored child").inc()
        strongConnect(w, appender, executables, blockers)

        // If our child is ineligible, we are ineligible. We return
        // immediately.
        if (!metadatas(w).eligible) {
          metadatas(v).eligible = false
          metrics.numChildrenVisited.observe(numChildren)
          return
        }

        metadatas(v).lowLink =
          Math.min(metadatas(v).lowLink, metadatas(w).lowLink)
      } else if (metadatas(w).stackIndex != -1) {
        metrics.strongConnectBranchTotal.labels("on stack child").inc()
        metadatas(v).lowLink =
          Math.min(metadatas(v).lowLink, metadatas(w).number)
      }
    }
    metrics.numChildrenVisited.observe(numChildren)

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
      executed.add(component)
      metrics.componentSize.observe(1)
    } else {
      val component = stack.slice(metadatas(v).stackIndex, stack.size)
      stack.remove(metadatas(v).stackIndex,
                   stack.size - metadatas(v).stackIndex)
      for (w <- component) {
        metadatas(w).stackIndex = -1
        executed.add(w)
      }
      appender.appendMany(
        executables,
        component.sortBy(k => (getVertex(k).get.sequenceNumber, k))
      )
      metrics.componentSize.observe(component.size)
    }
  }
}
