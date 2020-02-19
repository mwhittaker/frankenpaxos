package frankenpaxos.matchmakermultipaxos

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.Serializer
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

  case class DoubleLeaderReconfigurationState(
      firstReconfigurationTimer: Transport#Timer,
      acceptorFailureTimer: Transport#Timer,
      secondReconfigurationTimer: Transport#Timer
  ) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

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

  val state: State = workload match {
    case DoNothing =>
      DoNothingState

    case workload: DoubleLeaderReconfiguration =>
      // We reconfigure from one set of 2f+1 acceptors to a different set of
      // 2f+1 acceptors. Then, we fail one and swap it out. So, we need at
      // least 2(2f+1) acceptors.
      val n = 2 * config.f + 1
      logger.checkGe(acceptors.size, 2 * n)

      val firstReconfigurationTimer =
        timer(
          "firstReconfigurationTimer",
          workload.firstReconfigurationDelay,
          () => {
            logger.info("firstReconfigurationTimer triggered")
            leaders(0).send(
              LeaderInbound().withForceReconfiguration(
                ForceReconfiguration(acceptorIndex = n until 2 * n)
              )
            )
          }
        )
      val acceptorFailureTimer = timer(
        "acceptorFailureTimer",
        workload.acceptorFailureDelay,
        () => {
          logger.info("acceptorFailureTimer triggered")
          acceptors(n).send(AcceptorInbound().withDie(Die()))
        }
      )
      val secondReconfigurationTimer = timer(
        "secondReconfigurationTimer",
        workload.secondReconfigurationDelay,
        () => {
          logger.info("secondReconfigurationTimer triggered")
          leaders(0).send(
            LeaderInbound().withForceReconfiguration(
              ForceReconfiguration(
                acceptorIndex = Seq(0) ++ (n + 1 until 2 * n)
              )
            )
          )
        }
      )
      firstReconfigurationTimer.start()
      acceptorFailureTimer.start()
      secondReconfigurationTimer.start()
      DoubleLeaderReconfigurationState(
        firstReconfigurationTimer = firstReconfigurationTimer,
        acceptorFailureTimer = acceptorFailureTimer,
        secondReconfigurationTimer = secondReconfigurationTimer
      )
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    logger.fatal(
      "Driver received a message, but nobody should be sending to the Driver! "
    )
  }
}
