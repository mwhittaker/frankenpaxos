package frankenpaxos.spaxosdecouple

import frankenpaxos.Actor
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.statemachine
import frankenpaxos.statemachine.AppendLog
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.statemachine.StateMachine.read
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.duration

object SuperNodeMain extends App {
  case class Flags(
      // Basic flags.
      index: Int = -1,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      stateMachine: StateMachine = new statemachine.Noop(),
      leaderDelay: java.time.Duration = java.time.Duration.ofSeconds(3),
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      batcherOptions: BatcherOptions = BatcherOptions.default,
      proposerOptions: ProposerOptions = ProposerOptions.default,
      disseminatorOptions: DisseminatorOptions = DisseminatorOptions.default,
      leaderOptions: LeaderOptions = LeaderOptions.default,
      proxyLeaderOptions: ProxyLeaderOptions = ProxyLeaderOptions.default,
      acceptorOptions: AcceptorOptions = AcceptorOptions.default,
      replicaOptions: ReplicaOptions = ReplicaOptions.default,
      proxyReplicaOptions: ProxyReplicaOptions = ProxyReplicaOptions.default
  )

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def batcherOptionAction(
        f: (A, BatcherOptions) => BatcherOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(batcherOptions = f(x, flags.batcherOptions))
      )

    def proposerOptionAction(
                             f: (A, ProposerOptions) => ProposerOptions
                           ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(proposerOptions = f(x, flags.proposerOptions))
      )

    def leaderOptionAction(
        f: (A, LeaderOptions) => LeaderOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(leaderOptions = f(x, flags.leaderOptions))
      )

    def proxyLeaderOptionAction(
        f: (A, ProxyLeaderOptions) => ProxyLeaderOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) =>
          flags.copy(proxyLeaderOptions = f(x, flags.proxyLeaderOptions))
      )

    def acceptorOptionAction(
        f: (A, AcceptorOptions) => AcceptorOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(acceptorOptions = f(x, flags.acceptorOptions))
      )

    def disseminatorOptionAction(
                              f: (A, DisseminatorOptions) => DisseminatorOptions
                            ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(disseminatorOptions = f(x, flags.disseminatorOptions))
      )

    def replicaOptionAction(
        f: (A, ReplicaOptions) => ReplicaOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(replicaOptions = f(x, flags.replicaOptions))
      )

    def proxyReplicaOptionAction(
        f: (A, ProxyReplicaOptions) => ProxyReplicaOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) =>
          flags.copy(proxyReplicaOptions = f(x, flags.proxyReplicaOptions))
      )

    def electionOptionAction(
        f: (A, ElectionOptions) => ElectionOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) =>
          flags.copy(
            leaderOptions = flags.leaderOptions
              .copy(electionOptions = f(x, flags.leaderOptions.electionOptions))
          )
      )
  }

  val parser = new scopt.OptionParser[Flags]("") {
    help("help")

    // Basic flags.
    opt[Int]("index").required().action((x, f) => f.copy(index = x))
    opt[File]("config").required().action((x, f) => f.copy(configFile = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"-1 to disable")

    // Batcher options.
    opt[Int]("batcher.batchSize")
      .batcherOptionAction((x, o) => o.copy(batchSize = x))

    // Leader options.
    opt[java.time.Duration]("leader.resendPhase1asPeriod")
      .leaderOptionAction((x, o) => o.copy(resendPhase1asPeriod = x))
    opt[Int]("leader.flushPhase2asEveryN")
      .leaderOptionAction((x, o) => o.copy(flushPhase2asEveryN = x))
    opt[java.time.Duration]("leader.election.pingPeriod")
      .electionOptionAction((x, o) => o.copy(pingPeriod = x))
    opt[java.time.Duration]("leader.election.noPingTimeoutMin")
      .electionOptionAction((x, o) => o.copy(noPingTimeoutMin = x))
    opt[java.time.Duration]("leader.election.noPingTimeoutMax")
      .electionOptionAction((x, o) => o.copy(noPingTimeoutMax = x))

    // ProxyLeader options.
    opt[Int]("proxy_leader.flushPhase2asEveryN")
      .proxyLeaderOptionAction((x, o) => o.copy(flushPhase2asEveryN = x))
    opt[Int]("proxy_leader.flushValueChosensEveryN")
      .proxyLeaderOptionAction((x, o) => o.copy(flushValueChosensEveryN = x))

    // Proposer options.
    opt[Int]("proposer.flushForwardsEveryN")
      .proposerOptionAction((x, o) => o.copy(flushForwardsEveryN = x))
    opt[Int]("proposer.flushClientRequestsEveryN")
      .proposerOptionAction((x, o) => o.copy(flushClientRequestsEveryN = x))

    // Disseminator Options
    opt[Int]("disseminator.flushChosensEveryN")
      .disseminatorOptionAction((x, o) => o.copy(flushChosensEveryN = x))
    opt[Int]("disseminator.flushAcknowledgeEveryN")
      .disseminatorOptionAction((x, o) => o.copy(flushAcknowledgeEveryN = x))

    // Acceptor options.

    // Replica options.
    opt[Int]("replica.logGrowSize")
      .replicaOptionAction((x, o) => o.copy(logGrowSize = x))
    opt[Boolean]("replica.unsafeDontUseClientTable")
      .replicaOptionAction((x, o) => o.copy(unsafeDontUseClientTable = x))
    opt[Int]("replica.sendChosenWatermarkEveryNEntries")
      .replicaOptionAction(
        (x, o) => o.copy(sendChosenWatermarkEveryNEntries = x)
      )
    opt[java.time.Duration]("replica.recoverLogEntryMinPeriod")
      .replicaOptionAction((x, o) => o.copy(recoverLogEntryMinPeriod = x))
    opt[java.time.Duration]("replica.recoverLogEntryMaxPeriod")
      .replicaOptionAction((x, o) => o.copy(recoverLogEntryMaxPeriod = x))
    opt[Boolean]("replica.unsafeDontRecover")
      .replicaOptionAction((x, o) => o.copy(unsafeDontRecover = x))

    // ProxyReplica Options.
    opt[Int]("proxy_replica.flushEveryN")
      .proxyReplicaOptionAction((x, o) => o.copy(flushEveryN = x))
  }

  // Parse flags.
  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  val logger = new PrintLogger(flags.logLevel)
  val transport = new NettyTcpTransport(logger)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())

  // Sanity check the configuration.
  logger.check(
    config.batcherAddresses.isEmpty ||
      config.batcherAddresses.size == 2 * config.f + 1
  )
  logger.checkEq(config.leaderAddresses.size, 2 * config.f + 1)
  logger.checkEq(config.leaderElectionAddresses.size, 2 * config.f + 1)
  logger.checkEq(config.proposerAddresses.size, 2 * config.f + 1)
  logger.checkEq(config.proxyLeaderAddresses.size, 2 * config.f + 1)
  logger.checkEq(config.acceptorAddresses.size, 1)
  logger.checkEq(config.disseminatorAddresses.size, 1)
  logger.checkEq(config.acceptorAddresses(0).size, 2 * config.f + 1)
  logger.checkEq(config.disseminatorAddresses(0).size, 2 * config.f + 1)
  logger.checkEq(config.replicaAddresses.size, 2 * config.f + 1)
  logger.checkEq(config.proxyReplicaAddresses.size, 2 * config.f + 1)
  logger.checkEq(config.distributionScheme, Colocated)

  // Construct batcher. Batching is optional, so if no batcher addresses are
  // given, we do not start a batcher.
  if (!config.batcherAddresses.isEmpty) {
    val batcher = new Batcher[NettyTcpTransport](
      address = config.batcherAddresses(flags.index),
      transport = transport,
      logger = logger,
      config = config,
      options = flags.batcherOptions
    )
  }

  // Construct a proposer
  val proposer = new Proposer[NettyTcpTransport](
    address = config.proposerAddresses(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    options = flags.proposerOptions
  )

  // Construct proxy leader.
  val proxyLeader = new ProxyLeader[NettyTcpTransport](
    address = config.proxyLeaderAddresses(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    options = flags.proxyLeaderOptions
  )

  // Construct acceptor.
  val acceptor = new Acceptor[NettyTcpTransport](
    address = config.acceptorAddresses(0)(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    options = flags.acceptorOptions
  )

  // Construct disseminator
  val disseminator = new Disseminator[NettyTcpTransport](
    address = config.disseminatorAddresses(0)(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    options = flags.disseminatorOptions
  )

  // Construct replica.
  val replica = new Replica[NettyTcpTransport](
    address = config.replicaAddresses(flags.index),
    transport = transport,
    logger = logger,
    stateMachine = flags.stateMachine,
    config = config,
    options = flags.replicaOptions
  )

  // Construct proxy replica.
  val proxyReplica = new ProxyReplica[NettyTcpTransport](
    address = config.proxyReplicaAddresses(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    options = flags.proxyReplicaOptions
  )

  // Construct leader. We make sure to construct the leader last so that the
  // other nodes have a time to start up before they are contacted. We also
  // sleep for a bit to let all the acceptors start up properly. Is this good
  // code? No. But it works okay :)
  Thread.sleep(flags.leaderDelay.toMillis())
  val leader = new Leader[NettyTcpTransport](
    address = config.leaderAddresses(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    options = flags.leaderOptions
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
