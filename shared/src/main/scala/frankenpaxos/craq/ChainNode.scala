package frankenpaxos.craq

import com.google.protobuf.ByteString

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.{
  Collectors,
  Counter,
  PrometheusCollectors,
  Summary
}
import frankenpaxos.roundsystem.RoundSystem

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
    .name("craq_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("craq_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
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

  private val pendingWrites: mutable.Buffer[WriteBatch] = mutable.Buffer()
  val stateMachine: mutable.Map[String, String] = mutable.Map[String, String]()
  var versions: Int = 0

  // ChainNode channels.
  private val chainNodes: Seq[Chan[ChainNode[Transport]]] =
    for (address <- config.chainNodeAddresses)
      yield chan[ChainNode[Transport]](address, ChainNode.serializer)

  // The chain node's state.
  @JSExport
  protected var state: State = Active

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
    pendingWrites.append(writeBatch)

    if (!isTail) {
      chainNodes(nextIndex).send(
        ChainNodeInbound().withWriteBatch(writeBatch)
      )
    } else {
      for (command <- writeBatch.write) {
        stateMachine.put(command.key, command.value)
        val reply = stateMachine.get(command.key)
        val clientAddress = transport.addressSerializer
          .fromBytes(
            command.commandId.clientAddress.toByteArray()
          )
        val client =
          chan[Client[Transport]](clientAddress, Client.serializer)
        client.send(
          ClientInbound().withClientReply(
            ClientReply(command.commandId,
                        -1,
                        ByteString.copyFromUtf8(reply.getOrElse("default"))))
        )
        versions += 1
      }
      pendingWrites.remove(pendingWrites.indexOf(writeBatch))
      if (!isHead) {
        chainNodes(prevIndex).send(ChainNodeInbound().withAck(Ack(writeBatch)))
      }
    }
  }

  private def processReadBatch(readBatch: ReadBatch): Unit = {
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
        logger.info("Read is clean")
        val reply = stateMachine.get(read.key)
        val clientAddress = transport.addressSerializer
          .fromBytes(
            read.commandId.clientAddress.toByteArray()
          )
        val client =
          chan[Client[Transport]](clientAddress, Client.serializer)
        client.send(
          ClientInbound().withReadReply(
            ReadReply(read.commandId,
                      -1,
                      ByteString.copyFromUtf8(reply.getOrElse("default"))))
        )
        versions += 1
      }
    }
    // Send dirty reads to tail
    if (dirtyReads.nonEmpty) {
      chainNodes.last.send(
        ChainNodeInbound().withTailRead(TailRead(ReadBatch(dirtyReads.toSeq))))
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ChainNodeInbound.Request

    val label =
      inbound.request match {
        case Request.Write(_)      => "Write"
        case Request.WriteBatch(_) => "WriteBatch"
        case Request.Read(_)       => "Read"
        case Request.ReadBatch(_)  => "ReadBatch"
        case Request.Ack(_)        => "Ack"
        case Request.TailRead(_)   => "TailRead"
        case Request.Empty =>
          logger.fatal("Empty ChainNodeInbound encountered.")
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
          logger.fatal("Empty ChainNodeInbound encountered.")
      }
    }
  }

  private def handleWrite(
      src: Transport#Address,
      write: Write
  ): Unit = {
    processWriteBatch(
      WriteBatch(Seq(write))
    )
  }

  private def handleWriteBatch(
      src: Transport#Address,
      writeBatch: WriteBatch
  ): Unit = {
    processWriteBatch(writeBatch)
  }

  private def handleRead(
      src: Transport#Address,
      read: Read
  ): Unit = {
    processReadBatch(
      ReadBatch(Seq(read))
    )
  }

  private def handleReadBatch(
      src: Transport#Address,
      readBatch: ReadBatch
  ): Unit = {

    processReadBatch(readBatch)
  }

  private def handleTailRead(
      src: Transport#Address,
      tailRead: TailRead
  ): Unit = {
    for (command <- tailRead.readBatch.read) {
      val reply = stateMachine.get(command.key)
      val clientAddress = transport.addressSerializer
        .fromBytes(
          command.commandId.clientAddress.toByteArray()
        )
      val client =
        chan[Client[Transport]](clientAddress, Client.serializer)
      client.send(
        ClientInbound().withReadReply(
          ReadReply(command.commandId,
                    -1,
                    ByteString.copyFromUtf8(reply.getOrElse("default"))))
      )
      versions += 1
      //replyBuffer += Write(command.commandId, command.key, reply.get)
    }

  }

  private def handleAck(
      src: Transport#Address,
      ack: Ack
  ): Unit = {
    pendingWrites.remove(pendingWrites.indexOf(ack.writeBatch))
    for (write <- ack.writeBatch.write) {
      stateMachine.put(write.key, write.value)
    }
    if (!isHead) {
      chainNodes(prevIndex).send(ChainNodeInbound().withAck(ack))
    }
  }
}
