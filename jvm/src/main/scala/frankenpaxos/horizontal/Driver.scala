package frankenpaxos.horizontal

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.Serializer
import frankenpaxos.election.basic.Participant
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.quorums.QuorumSystem
import frankenpaxos.quorums.QuorumSystemProto
import frankenpaxos.quorums.SimpleMajority
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object DriverInboundSerializer extends Serializer[Unit] {
  type A = Unit
  override def toBytes(x: A): Array[Byte] = Array[Byte]()
  override def fromBytes(bytes: Array[Byte]): A = ()
  override def toPrettyString(x: A): String = "()"
}

@JSExportAll
class Driver[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    workload: DriverWorkload
) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  // The Driver doesn't receive messages, only sends them.
  override type InboundMessage = Unit
  override val serializer = DriverInboundSerializer

  type AcceptorIndex = Int

  sealed trait State

  object DoNothingState extends State

  case class RepeatedLeaderReconfigurationState(
      delayTimer: Transport#Timer,
      reconfigureTimer: Transport#Timer
  ) extends State

  case class LeaderReconfigurationState(
      reconfigurationWarmupDelayTimer: Transport#Timer,
      reconfigurationWarmupTimer: Transport#Timer,
      reconfigurationDelayTimer: Transport#Timer,
      reconfigurationTimer: Transport#Timer,
      failureTimer: Transport#Timer,
      recoverTimer: Transport#Timer
  ) extends State

  case class LeaderFailureState(
      leaderChangeWarmupDelayTimer: Transport#Timer,
      leaderChangeWarmupTimer: Transport#Timer,
      failureTimer: Transport#Timer
  ) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Leader election participant channels.
  private val participants: Seq[Chan[Participant[Transport]]] =
    for (a <- config.leaderElectionAddresses)
      yield chan[Participant[Transport]](a, Participant.serializer)

  // Acceptor channels.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (a <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](a, Acceptor.serializer)

  // Replica channels.
  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (a <- config.replicaAddresses)
      yield chan[Replica[Transport]](a, Replica.serializer)

  def randomSubset(n: Int, m: Int): Set[Int] = {
    Random
      .shuffle(List() ++ (0 until n))
      .take(m)
      .toSet,
  }

  private def delayedTimer(
      name: String,
      delay: java.time.Duration,
      period: java.time.Duration,
      n: Int,
      f: () => Unit,
      onLast: () => Unit
  ): (Transport#Timer, Transport#Timer) = {
    case class Counter(var x: Int)

    val counter = Counter(n)
    lazy val t: Transport#Timer =
      timer(
        name,
        period,
        () => {
          if (counter.x > 1) {
            f()
            counter.x = counter.x - 1
            t.start()
          } else {
            onLast()
          }
        }
      )

    val delayTimer = timer(name + " delay", delay, () => { t.start() })
    delayTimer.start()

    (delayTimer, t)
  }

  private def reconfigure(leader: Int, acceptors: Set[Int]): Unit = {
    leaders(leader).send(
      LeaderInbound().withReconfigure(
        Reconfigure(
          Configuration(
            quorumSystem = QuorumSystem.toProto(new SimpleMajority(acceptors))
          )
        )
      )
    )
  }

  private def becomeLeader(leader: Int): Unit = {
    participants(leader).send(
      frankenpaxos.election.basic
        .ParticipantInbound()
        .withForceNoPing(
          frankenpaxos.election.basic.ForceNoPing()
        )
    )
  }

  private def repeatedLeaderReconfiguration(
      workload: RepeatedLeaderReconfiguration
  ): RepeatedLeaderReconfigurationState = {
    lazy val reconfigureTimer: Transport#Timer = timer(
      "reconfigureTimer",
      workload.period,
      () => {
        logger.info("reconfigureTimer triggered")
        reconfigure(0, Set() ++ (0 until (2 * config.f + 1)))
        reconfigureTimer.start()
      }
    )

    val delayTimer = timer(
      "delayTimer",
      workload.delay,
      () => {
        logger.info("delayTimer triggered")
        reconfigureTimer.start()
      }
    )
    delayTimer.start()

    RepeatedLeaderReconfigurationState(
      delayTimer = delayTimer,
      reconfigureTimer = reconfigureTimer
    )
  }

  private def leaderReconfiguration(
      workload: LeaderReconfiguration
  ): LeaderReconfigurationState = {
    val (reconfigurationWarmupDelayTimer, reconfigurationWarmupTimer) =
      delayedTimer(
        name = "LeaderReconfiguration reconfiguration warmup",
        delay = workload.reconfigurationWarmupDelay,
        period = workload.reconfigurationWarmupPeriod,
        n = workload.reconfigurationWarmupNum,
        f = () => {
          logger.info(
            "LeaderReconfiguration reconfiguration warmup triggered!"
          )
          reconfigure(0, randomSubset(acceptors.size, 2 * config.f + 1))
        },
        onLast = () => {
          logger.info(
            "LeaderReconfiguration reconfiguration warmup triggered!"
          )
          reconfigure(0, randomSubset(acceptors.size, 2 * config.f + 1))
        }
      )

    val (reconfigurationDelayTimer, reconfigurationTimer) = delayedTimer(
      name = "LeaderReconfiguration reconfiguration",
      delay = workload.reconfigurationDelay,
      period = workload.reconfigurationPeriod,
      n = workload.reconfigurationNum,
      f = () => {
        logger.info("LeaderReconfiguration reconfiguration triggered!")
        reconfigure(0, randomSubset(acceptors.size, 2 * config.f + 1))
      },
      onLast = () => {
        logger.info("LeaderReconfiguration reconfiguration triggered!")
        reconfigure(0, Set() ++ (0 until 2 * config.f + 1))
      }
    )

    val failureTimer = timer(
      "failure",
      workload.failureDelay,
      () => {
        logger.info("LeaderReconfiguration failure triggered!")
        acceptors(0).send(AcceptorInbound().withDie(Die()))
      }
    )
    failureTimer.start()

    val recoverTimer = timer(
      "recover",
      workload.recoverDelay,
      () => {
        logger.info("LeaderReconfiguration recover triggered!")
        reconfigure(0, Set() ++ (1 to 2 * config.f + 1))
      }
    )
    recoverTimer.start()

    LeaderReconfigurationState(
      reconfigurationWarmupDelayTimer = reconfigurationWarmupDelayTimer,
      reconfigurationWarmupTimer = reconfigurationWarmupTimer,
      reconfigurationDelayTimer = reconfigurationDelayTimer,
      reconfigurationTimer = reconfigurationTimer,
      failureTimer = failureTimer,
      recoverTimer = recoverTimer
    )
  }

  private def leaderFailure(
      workload: LeaderFailure
  ): LeaderFailureState = {
    val (leaderChangeWarmupDelayTimer, leaderChangeWarmupTimer) =
      delayedTimer(
        name = "leader change warmup",
        delay = workload.leaderChangeWarmupDelay,
        period = workload.leaderChangeWarmupPeriod,
        n = workload.leaderChangeWarmupNum,
        f = () => {
          logger.info(
            "LeaderFailure leader change warmup triggered!"
          )
          // I found that we need to let the second leader get some warmup in,
          // so we actually just change to it again and again.
          becomeLeader(1)
        },
        onLast = () => {
          logger.info(
            "LeaderFailure leader change warmup triggered!"
          )
          becomeLeader(0)
        }
      )

    val failureTimer = timer(
      "failure",
      workload.failureDelay,
      () => {
        logger.info("LeaderFailure failure triggered!")
        leaders(0).send(LeaderInbound().withDie(Die()))
      }
    )
    failureTimer.start()

    LeaderFailureState(
      leaderChangeWarmupDelayTimer = leaderChangeWarmupDelayTimer,
      leaderChangeWarmupTimer = leaderChangeWarmupTimer,
      failureTimer = failureTimer
    )
  }

  val state: State = workload match {
    case DoNothing =>
      DoNothingState
    case workload: RepeatedLeaderReconfiguration =>
      repeatedLeaderReconfiguration(workload)
    case workload: LeaderReconfiguration =>
      leaderReconfiguration(workload)
    case workload: LeaderFailure =>
      leaderFailure(workload)
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    logger.fatal(
      "Driver received a message, but nobody should be sending to the Driver!"
    )
  }
}
