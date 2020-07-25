package frankenpaxos.craq

import com.google.protobuf.ByteString

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.election.basic.Participant
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.KeyValueStore

import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object ChainNodeInboundSerializer extends ProtoSerializer[ChainNodeInbound] {
  type A = ChainNodeInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object ChainNode {
  val serializer = ChainNodeInboundSerializer
}

@JSExportAll
case class ChainNodeOptions(
                          measureLatencies: Boolean
                        )

@JSExportAll
object ChainNodeOptions {
  val default = ChainNodeOptions(
    measureLatencies = true
  )
}

@JSExportAll
class ChainNodeMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val leaderChangesTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_leader_leader_changes_total")
    .help("Total number of leader changes.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_leader_resend_phase1as_total")
    .help("Total number of times the leader resent phase 1a messages.")
    .register()

  val noopsFlushedTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_leader_noops_flushed_total")
    .help("Total number of times the leader flushed a noop.")
    .register()
}

@JSExportAll
class ChainNode[Transport <: frankenpaxos.Transport[Transport]](
                                                              address: Transport#Address,
                                                              transport: Transport,
                                                              logger: Logger,
                                                              config: Config[Transport],
                                                              options: ChainNodeOptions = ChainNodeOptions.default,
                                                              metrics: ChainNodeMetrics = new ChainNodeMetrics(PrometheusCollectors),
                                                              seed: Long = System.currentTimeMillis()
                                                            ) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ChainNodeInbound
  override val serializer = ChainNodeInboundSerializer

  type ClientId = Int
  type ClientPseudonym = Int

  type AcceptorIndex = Int
  type Round = Int
  type Slot = Int

  @JSExportAll
  sealed trait State

  @JSExportAll
  case object Inactive extends State

  @JSExportAll
  case object Active extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  // The client table used to ensure exactly once execution semantics. Every
  // entry in the client table is keyed by a clients address and its pseudonym
  // and maps to the largest executed id for the client and the result of
  // executing the command. Note that unlike with generalized protocols like
  // BPaxos and EPaxos, we don't need to use the more complex ClientTable
  // class. A simple map suffices.
  @JSExport
  protected var clientTable =
  mutable.Map[(ByteString, ClientPseudonym), (ClientId, ByteString)]()

  private val index = config.chainNodeAddresses.indexOf(address)
  private val nextIndex = index + 1
  private val prevIndex = index - 1
  private val isHead = index == 0
  private val isTail = index == config.chainNodeAddresses.size - 1

  private var pendingWrites: mutable.Buffer[WriteBatch] = mutable.Buffer()
  val stateMachine: mutable.Map[String, String] = mutable.Map[String, String]()

  // ChainNode channels.
  private val chainNodes: Seq[Chan[ChainNode[Transport]]] =
    for (address <- config.chainNodeAddresses)
      yield chan[ChainNode[Transport]](address, ChainNode.serializer)

  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  // If the leader is the active leader, then this is its round. If it is
  // inactive, then this is the largest active round it knows about.
  @JSExport
  protected var round: Round = roundSystem
    .nextClassicRound(leaderIndex = 0, round = -1)

  // The next available slot in the log. Even though we have a next slot into
  // the log, you'll note that we don't even have a log! Because we've
  // decoupled aggressively, leaders don't actually need a log at all.
  @JSExport
  protected var nextSlot: Slot = 0

  // Every slot less than chosenWatermark has been chosen. Replicas
  // periodically send their chosenWatermarks to the leaders.
  @JSExport
  protected var chosenWatermark: Slot = 0

  // Leader election address. This field exists for the javascript
  // visualizations.
  @JSExport
  protected val electionAddress = config.chainNodeAddresses(index)

  // The number of Phase2a messages since the last flush.
  private var numPhase2asSentSinceLastFlush: Int = 0

  // A MultiPaxos leader sends messages to ProxyLeaders one at a time.
  // `currentProxyLeader` is the proxy leader that is currently being sent to.
  // Note that this value is only used if config.distributionScheme is Hash.
  private var currentProxyLeader: Int = 0

  // The leader's state.
  @JSExport
  protected var state: State = if (index == 0) {
    Active
  } else {
    Inactive
  }

  // Timers ////////////////////////////////////////////////////////////////////

  // Helpers ///////////////////////////////////////////////////////////////////
  private def timed[T](label: String)(e: => T): T = {
    if (options.measureLatencies) {
      val startNanos = System.nanoTime
      val x = e
      val stopNanos = System.nanoTime
      metrics.requestsLatency
        .labels(label)
        .observe((stopNanos - startNanos).toDouble / 1000000)
      x
    } else {
      e
    }
  }

  private def processWriteBatch(writeBatch: WriteBatch): Unit = {
    state match {
      case Inactive =>
        logger.fatal(
          s"A leader tried to process a client request batch but is not in " +
            s"Phase 2. It's state is $state."
        )

      case Active =>
    }

    pendingWrites.append(writeBatch)

    if (!isTail) {
      chainNodes(nextIndex).send(
        ChainNodeInbound().withWriteBatch(writeBatch)
      )
    } else {
      for (command <- writeBatch.write) {
        val reply = stateMachine.put(command.key, command.value)
        val clientAddress = transport.addressSerializer
          .fromBytes(
            command.commandId.clientAddress.toByteArray()
          )
        val client =
          chan[Client[Transport]](clientAddress, Client.serializer)
        client.send(
          ClientInbound().withClientReply(ClientReply(command.commandId, -1, ByteString.copyFromUtf8(reply.getOrElse(""))))
        )
      }
      if (!isHead) {
        chainNodes(prevIndex).send(ChainNodeInbound().withAck(Ack(writeBatch)))
      }
    }
  }

  private def processReadBatch(readBatch: ReadBatch): Unit = {
    state match {
      case Inactive =>
        logger.fatal(
          s"A leader tried to process a client request batch but is not in " +
            s"Phase 2. It's state is $state."
        )

      case Active =>
        val keys: mutable.Set[String] = mutable.Set[String]()
        for (pw <- pendingWrites) {
          for (w <- pw.write) {
            keys.add(w.key)
          }
        }

        val dirtyReads: mutable.Buffer[Read] = mutable.Buffer[Read]()
        for (read <- readBatch.read) {
          if (keys.contains(read.key)) {
            // Key is dirty ask the tail
            dirtyReads += read
          } else {
            // Key is clean serve latest value
            val reply = stateMachine.get(read.key)
            val clientAddress = transport.addressSerializer
              .fromBytes(
                read.commandId.clientAddress.toByteArray()
              )
            val client =
              chan[Client[Transport]](clientAddress, Client.serializer)
            client.send(
              ClientInbound().withClientReply(ClientReply(read.commandId, -1, ByteString.copyFromUtf8(reply.getOrElse(""))))
            )
          }
        }
        // Send dirty reads to tail
        if (dirtyReads.nonEmpty) {
          chainNodes.last.send(ChainNodeInbound().withTailRead(TailRead(ReadBatch(dirtyReads.toSeq))))
        }
    }

  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ChainNodeInbound.Request

    val label =
      inbound.request match {
        case Request.Write(_)            => "Write"
        case Request.WriteBatch(_)            => "WriteBatch"
        case Request.Read(_)            => "Read"
        case Request.ReadBatch(_)            => "ReadBatch"
        case Request.Ack(_)            => "Ack"
        case Request.TailRead(_)        => "TailRead"
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Write(r) =>
          handleWrite(src, r)
        case Request.WriteBatch(r) =>
          handleWriteBatch(src, r)
        case Request.Read(r) =>
          handleRead(src, r)
        case Request.ReadBatch(r) =>
          handleReadBatch(src, r)
        case Request.Ack(r) =>
          handleAck(src, r)
        case Request.TailRead(r) =>
          handleTailRead(src, r)
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handleWrite(
                                   src: Transport#Address,
                                   write: Write
                                 ): Unit = {
    state match {
      case Inactive =>
        // If we're not the active leader but receive a request from a client,
        // then we send back a NotLeader message to let them know that they've
        // contacted the wrong leader.
      case Active =>
        processWriteBatch(
          WriteBatch(
            Seq(write))
          )
    }
  }

  private def handleWriteBatch(
                                        src: Transport#Address,
                                        writeBatch: WriteBatch
                                      ): Unit = {
    state match {
      case Inactive =>

      case Active =>
        processWriteBatch(writeBatch)
    }
  }

    private def handleRead(
                             src: Transport#Address,
                             read: Read
                           ): Unit = {
      state match {
        case Inactive =>
          // If we're not the active leader but receive a request from a client,
          // then we send back a NotLeader message to let them know that they've
          // contacted the wrong leader.

        case Active =>
          processReadBatch(
            ReadBatch(
              Seq(read))
          )
      }
    }

    private def handleReadBatch(
                                  src: Transport#Address,
                                  readBatch: ReadBatch
                                ): Unit = {
      state match {
        case Inactive =>

        case Active =>
          processReadBatch(readBatch)
      }
    }

  private def handleTailRead(
                               src: Transport#Address,
                               tailRead: TailRead
                             ): Unit = {
    state match {
      case Inactive =>

      case Active =>
        //val replyBuffer = mutable.Buffer[Write]()
        for (command <- tailRead.readBatch.read) {
          val reply = stateMachine.get(command.key)
          val clientAddress = transport.addressSerializer
            .fromBytes(
              command.commandId.clientAddress.toByteArray()
            )
          val client =
            chan[Client[Transport]](clientAddress, Client.serializer)
          client.send(
            ClientInbound().withClientReply(ClientReply(command.commandId, -1, ByteString.copyFromUtf8(reply.getOrElse(""))))
          )
          //replyBuffer += Write(command.commandId, command.key, reply.get)
        }

    }
  }

  private def handleAck(
                         src: Transport#Address,
                         ack: Ack
                       ): Unit = {
    state match {
      case Inactive =>

      case Active =>
        pendingWrites.remove(pendingWrites.indexOf(ack.writeBatch))
        for (write <- ack.writeBatch.write) {
          stateMachine.put(write.key, write.value)
        }
    }
  }
}
