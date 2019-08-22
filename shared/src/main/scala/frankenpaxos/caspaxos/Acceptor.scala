package frankenpaxos.caspaxos

import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object AcceptorInboundSerializer extends ProtoSerializer[AcceptorInbound] {
  type A = AcceptorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Acceptor {
  val serializer = AcceptorInboundSerializer
}

@JSExportAll
class Acceptor[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = AcceptorInbound
  override def serializer = Acceptor.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and compute our index.
  logger.check(config.acceptorAddresses.contains(address))
  private val index = config.acceptorAddresses.indexOf(address)

  // The largest round this acceptor has ever heard of.
  @JSExport
  protected var round: Int = -1

  // The largest round this acceptor has voted in.
  @JSExport
  protected var voteRound: Int = -1

  // The value voted for in voteRound. In this simple implementation of
  // CASPaxos, the state is a set of integers.
  @JSExport
  protected var voteValue: Option[Set[Int]] = None

  // Helpers ///////////////////////////////////////////////////////////////////
  private def toIntSetProto(xs: Set[Int]): IntSet = IntSet(value = xs.toSeq)

  private def fromIntSetProto(xs: IntSet): Set[Int] = xs.value.toSet

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: AcceptorInbound
  ): Unit = {
    import AcceptorInbound.Request
    inbound.request match {
      case Request.Phase1A(r) => handlePhase1a(src, r)
      case Request.Phase2A(r) => handlePhase2a(src, r)
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
  }

  private def handlePhase1a(
      src: Transport#Address,
      phase1a: Phase1a
  ): Unit = {
    val leader = chan[Leader[Transport]](src, Leader.serializer)

    // Nack messages from earlier rounds.
    if (phase1a.round < round) {
      logger.debug(
        s"Acceptor received a Phase1a in round ${phase1a.round} but is " +
          s"already in round $round. The acceptor is sending back a nack."
      )
      leader.send(LeaderInbound().withNack(Nack(higherRound = round)))
    }

    round = phase1a.round
    leader.send(
      LeaderInbound().withPhase1B(
        Phase1b(round = round,
                acceptorIndex = index,
                voteRound = voteRound,
                voteValue = voteValue.map(toIntSetProto))
      )
    )
  }

  private def handlePhase2a(
      src: Transport#Address,
      phase2a: Phase2a
  ): Unit = {
    val leader = chan[Leader[Transport]](src, Leader.serializer)

    // Nack messages from earlier rounds.
    if (phase2a.round < round) {
      logger.debug(
        s"Acceptor received a Phase2a in round ${phase2a.round} but is " +
          s"already in round $round. The acceptor is sending back a nack."
      )
      leader.send(LeaderInbound().withNack(Nack(higherRound = round)))
    }

    round = round
    voteRound = round
    voteValue = Some(fromIntSetProto(phase2a.value))
    leader.send(
      LeaderInbound().withPhase2B(Phase2b(round = round, acceptorIndex = index))
    )
  }
}
