package frankenpaxos.matchmakermultipaxos

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

  case class MatchmakerReconfigurationState(
      reconfigurationWarmupDelayTimer: Transport#Timer,
      reconfigurationWarmupTimer: Transport#Timer,
      matchmakerReconfigurationDelayTimer: Transport#Timer,
      matchmakerReconfigurationTimer: Transport#Timer,
      failureTimer: Transport#Timer,
      recoverTimer: Transport#Timer,
      reconfigureTimer: Transport#Timer
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

  // Matchmaker channels.
  private val matchmakers: Seq[Chan[Matchmaker[Transport]]] =
    for (a <- config.matchmakerAddresses)
      yield chan[Matchmaker[Transport]](a, Matchmaker.serializer)

  // Reconfigurer channels.
  private val reconfigurers: Seq[Chan[Reconfigurer[Transport]]] =
    for (a <- config.reconfigurerAddresses)
      yield chan[Reconfigurer[Transport]](a, Reconfigurer.serializer)

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

  def reconfigure(leader: Int, acceptors: Set[Int]): Unit = {
    leaders(leader).send(
      LeaderInbound().withForceReconfiguration(
        ForceReconfiguration(acceptorIndex = acceptors.toSeq)
      )
    )
  }

  def matchmakerReconfigure(reconfigurer: Int, matchmakers: Set[Int]): Unit = {
    reconfigurers(reconfigurer).send(
      ReconfigurerInbound().withForceMatchmakerReconfiguration(
        ForceMatchmakerReconfiguration(matchmakerIndex = matchmakers.toSeq)
      )
    )
  }

  def becomeLeader(leader: Int): Unit = {
    participants(leader).send(
      frankenpaxos.election.basic
        .ParticipantInbound()
        .withForceNoPing(
          frankenpaxos.election.basic.ForceNoPing()
        )
    )
  }

  val state: State = workload match {
    case DoNothing =>
      DoNothingState

    case workload: RepeatedLeaderReconfiguration =>
      lazy val reconfigureTimer: Transport#Timer =
        timer(
          "reconfigureTimer",
          workload.period,
          () => {
            logger.info("reconfigureTimer triggered")
            leaders(0).send(
              LeaderInbound().withForceReconfiguration(
                ForceReconfiguration(acceptorIndex = 0 until (2 * config.f + 1))
              )
            )
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

    case workload: LeaderReconfiguration =>
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

    case workload: MatchmakerReconfiguration =>
      val (reconfigurationWarmupDelayTimer, reconfigurationWarmupTimer) =
        delayedTimer(
          name = "reconfiguration warmup",
          delay = workload.reconfigurationWarmupDelay,
          period = workload.reconfigurationWarmupPeriod,
          n = workload.reconfigurationWarmupNum,
          f = () => {
            logger.info(
              "MatchmakerReconfiguration reconfiguration warmup triggered!"
            )
            reconfigure(0, Set() ++ (0 until 2 * config.f + 1))
          },
          onLast = () => {
            logger.info(
              "MatchmakerReconfiguration reconfiguration warmup triggered!"
            )
            reconfigure(0, Set() ++ (0 until 2 * config.f + 1))
          }
        )

      val (matchmakerReconfigurationDelayTimer,
           matchmakerReconfigurationTimer) =
        delayedTimer(
          name = "reconfiguration warmup",
          delay = workload.matchmakerReconfigurationDelay,
          period = workload.matchmakerReconfigurationPeriod,
          n = workload.matchmakerReconfigurationNum,
          f = () => {
            logger.info(
              "MatchmakerReconfiguration matchmaker reconfiguration triggered!"
            )
            matchmakerReconfigure(
              0,
              randomSubset(matchmakers.size, 2 * config.f + 1)
            )
          },
          onLast = () => {
            logger.info(
              "MatchmakerReconfiguration matchmaker reconfiguration triggered!"
            )
            matchmakerReconfigure(0, Set() ++ (1 to 2 * config.f + 1))
          }
        )

      val failureTimer = timer(
        "failure",
        workload.failureDelay,
        () => {
          logger.info("MatchmakerReconfiguration failure triggered!")
          matchmakers(2 * config.f + 1).send(MatchmakerInbound().withDie(Die()))
        }
      )
      failureTimer.start()

      val recoverTimer = timer(
        "recover",
        workload.recoverDelay,
        () => {
          logger.info("MatchmakerReconfiguration recover triggered!")
          matchmakerReconfigure(0, Set() ++ (0 until 2 * config.f + 1))
        }
      )
      recoverTimer.start()

      val reconfigureTimer = timer(
        "reconfigure",
        workload.reconfigureDelay,
        () => {
          logger.info("MatchmakerReconfiguration reconfigure triggered!")
          reconfigure(0, Set() ++ (0 until 2 * config.f + 1))
        }
      )
      reconfigureTimer.start()

      MatchmakerReconfigurationState(
        reconfigurationWarmupDelayTimer = reconfigurationWarmupDelayTimer,
        reconfigurationWarmupTimer = reconfigurationWarmupTimer,
        matchmakerReconfigurationDelayTimer =
          matchmakerReconfigurationDelayTimer,
        matchmakerReconfigurationTimer = matchmakerReconfigurationTimer,
        failureTimer = failureTimer,
        recoverTimer = recoverTimer,
        reconfigureTimer = reconfigureTimer
      )

    case workload: LeaderFailure =>
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
            becomeLeader(Random.nextInt(participants.size))
          },
          onLast = () => {
            logger.info(
              "MatchmakerReconfiguration reconfiguration warmup triggered!"
            )
            becomeLeader(0)
          }
        )

      val failureTimer = timer(
        "failure",
        workload.failureDelay,
        () => {
          logger.info("MatchmakerReconfiguration failure triggered!")
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

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    logger.fatal(
      "Driver received a message, but nobody should be sending to the Driver! "
    )
  }
}
