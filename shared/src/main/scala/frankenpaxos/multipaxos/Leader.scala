package frankenpaxos.multipaxos

import scala.collection.mutable
import scala.scalajs.js.annotation._
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Chan

@JSExportAll
object LeaderInboundSerializer extends ProtoSerializer[LeaderInbound] {
  type A = LeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer
}

case class CommanderId(slot: Integer, command: String)

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

  // Verify the Paxos config and get our proposer index.
  logger.check(config.valid())
  //logger.check(config.proposerAddresses.contains(address))
  //private val index: Int = config.proposerAddresses.indexOf(address)

  // Monotonically increasing (equivalent to round)
  @JSExport
  protected var ballotNumber: Double = 0

  // Whether the leader is active
  @JSExport
  protected var active: Boolean = true

  // A mapping between slot number to proposed command for that slot
  @JSExport
  protected var proposals: scala.collection.mutable.Map[Int, String] =
    scala.collection.mutable.Map()

  // For each commander spawned keep track of the acceptors who have not responded
  @JSExport
  protected var waitForCommander
    : scala.collection.mutable.Map[CommanderId, Set[Transport#Address]] =
    scala.collection.mutable.Map()

  // Keep track of acceptors who have not responded to Phase 1a requests
  // TODO(neil): Is this set ever cleared when the round is increased?
  @JSExport
  protected var waitForScout: Set[Transport#Address] =
    config.acceptorAddresses.toSet

  // Keep track of acceptor proposals gathered by scout
  @JSExport
  protected var scoutProposalValues: Set[ProposedValue] = Set()

  // Whether to start up the scout first
  @JSExport
  protected var activateScout: Boolean = true

  // Connections to the acceptors.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (acceptorAddress <- config.acceptorAddresses)
      yield
        chan[Acceptor[Transport]](
          acceptorAddress,
          Acceptor.serializer
        )

  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (replicaAddress <- config.replicaAddresses)
      yield
        chan[Replica[Transport]](
          replicaAddress,
          Replica.serializer
        )

  // A list of the clients awaiting a response.
  private val clients: mutable.Buffer[
    Chan[Client[Transport]]
  ] = mutable.Buffer()

  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    import LeaderInbound.Request
    if (activateScout) {
      scoutProcess()
      activateScout = false
    }
    inbound.request match {
      case Request.ProposeRequest(r) => handleProposeRequest(src, r)
      case Request.Adopted(r)        => handleAdopted(src, r)
      case Request.Preempted(r)      => handlePreempted(src, r)
      case Request.Phase1B(r)        => handlePhase1b(src, r)
      case Request.Phase2B(r)        => handlePhase2b(src, r)
      case Request.Empty => {
        logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handleProposeRequest(
      src: Transport#Address,
      request: ProposeToLeader
  ): Unit = {
    if (proposals.get(request.slot).isEmpty) {
      proposals.put(request.slot, request.command)
      if (active) {
        //println("Launching commander process")
        commanderProcess(slotOne = request.slot, commandOne = request.command)
      }
    }

  }

  private def maxProposal(proposals: Seq[ProposedValue]): ProposedValue = {
    var maxBallot: Double = -1
    var maxProposal: ProposedValue = null

    for (proposal <- proposals) {
      if (proposal.ballot > maxBallot) {
        maxBallot = proposal.ballot
        maxProposal = proposal
      }
    }

    maxProposal
  }

  // TODO(mwhittaker): Do all the proposals in request have the same slot?
  private def handleAdopted(src: Transport#Address, request: Adopted): Unit = {
    val proposal: ProposedValue = maxProposal(request.proposals)
    if (proposal == null) {
      active = true
      return
    }
    proposals.put(proposal.slot, proposal.command)

    for (slot <- proposals.keysIterator) {
      commanderProcess(slot, proposals.get(slot).get)
    }
    active = true
  }

  private def handlePreempted(
      src: Transport#Address,
      request: Preempted
  ): Unit = {
    if (request.ballot > ballotNumber) {
      active = false
      ballotNumber = request.ballot + 1
      scoutProcess()
    }
  }

  private def scoutProcess(): Unit = {
    for (acceptor <- acceptors.toIterator) {
      acceptor.send(
        AcceptorInbound().withPhase1A(
          Phase1a(leaderId = 0, ballot = ballotNumber)
        )
      )
    }
  }

  private def commanderProcess(slotOne: Integer, commandOne: String): Unit = {
    for (acceptor <- acceptors.toIterator) {
      //println("Corrupt wire")
      acceptor.send(
        AcceptorInbound().withPhase2A(
          Phase2a(
            leaderId = 0,
            proposal = ProposedValue(
              ballot = ballotNumber,
              slot = slotOne,
              command = commandOne
            )
          )
        )
      )
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      request: Phase1b
  ): Unit = {
    //println("An acceptor responded to the leader in phase 1b")
    val leader = chan[Leader[Transport]](
      address,
      Leader.serializer
    )
    if (request.ballot == ballotNumber) {
      scoutProposalValues = scoutProposalValues.union(request.proposals.toSet)
      waitForScout -= src
      if (waitForScout.size < (acceptors.size / 2)) {
        // TODO(neil): Can we call the handleAdopted method directly here? Why
        // do we need to send it? -Michael.
        leader.send(
          LeaderInbound().withAdopted(
            Adopted(
              ballot = ballotNumber,
              proposals = scoutProposalValues.toSeq
            )
          )
        )
      }
    } else {
      // TODO(neil): Can we call the handlePrempted method directly here? Why
      // do we need to send it? -Michael.
      leader.send(
        LeaderInbound().withPreempted(
          Preempted(ballot = request.ballot, leaderId = 0)
        )
      )
    }
  }

  private def handlePhase2b(
      src: Transport#Address,
      request: Phase2b
  ): Unit = {
    if (request.ballot == ballotNumber) {
      val commanderId: CommanderId = CommanderId(
        request.phase2AProposal.slot,
        request.phase2AProposal.command
      )
      if (waitForCommander.get(commanderId).isEmpty) {
        waitForCommander.put(commanderId, config.acceptorAddresses.toSet)
      }

      var acceptorSet: Set[Transport#Address] =
        waitForCommander.get(commanderId).get
      acceptorSet -= src

      waitForCommander.put(commanderId, acceptorSet)
      if (acceptorSet.size < (acceptors.size / 2)) {
        for (replica <- replicas) {
          //println("Command decided at the leader level")
          replica.send(
            ReplicaInbound().withDecision(
              Decision(
                slot = request.phase2AProposal.slot,
                command = request.phase2AProposal.command
              )
            )
          )
        }
      }
    } else {
      val leader = chan[Leader[Transport]](
        address,
        Leader.serializer
      )
      leader.send(
        LeaderInbound().withPreempted(
          Preempted(ballot = request.ballot, leaderId = 0)
        )
      )
    }
  }
}
