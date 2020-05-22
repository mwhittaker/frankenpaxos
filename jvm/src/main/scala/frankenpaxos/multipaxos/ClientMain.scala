package frankenpaxos.multipaxos

import frankenpaxos.Actor
import frankenpaxos.BenchmarkUtil
import frankenpaxos.FileLogger
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.monitoring.PrometheusCollectors
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random

object ClientMain extends App {
  sealed trait ReadConsistency
  case object Linearizable extends ReadConsistency
  case object Sequential extends ReadConsistency
  case object Eventual extends ReadConsistency
  implicit val read: scopt.Read[ReadConsistency] = scopt.Read.reads({
    case "linearizable" => Linearizable
    case "sequential"   => Sequential
    case "eventual"     => Eventual
  })

  case class Flags(
      // Basic flags.
      host: String = "localhost",
      port: Int = 9000,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Benchmark flags.
      measurementGroupSize: Int = 1,
      warmupDuration: java.time.Duration = java.time.Duration.ofSeconds(5),
      warmupTimeout: Duration = 10 seconds,
      warmupSleep: java.time.Duration = java.time.Duration.ofSeconds(0),
      numWarmupClients: Int = 10,
      duration: java.time.Duration = java.time.Duration.ofSeconds(5),
      timeout: Duration = 10 seconds,
      numClients: Int = 1,
      outputFilePrefix: String = "",
      readConsistency: ReadConsistency = Linearizable,
      // Workload flags.
      //
      // If we say a workload is "90% reads", that can mean one of two things.
      //
      //   1. It could mean that every client repeatedly flips a coin that
      //      lands heads 90% of the time. If the coin lands heads, the client
      //      reads; otherwise, it writes.
      //   2. It could mean that 90% of clients are predetermined to only read
      //      while 10% of clients are predetermined to only write.
      //
      // Option 1 is more natural and I think is what people typically think
      // when they hear about a "90% read" workload. Option 1, however, can be
      // a little annoying for performance debugging. If writes are slow, it
      // can stall reads. Option 2 leads allows reads and writes to operate
      // more independently, which lets us debug a little better.
      //
      // If predeterminedReadFraction is -1, then option 1 is used and
      // `workload` is used. Otherwise, predeterminedReadFraction must fall in
      // the range 0 to 100 and specifies the fraction of clients that are
      // predetermined to read. These clients use `readWorkload`. The writes
      // use `writeWorkload`.
      predeterminedReadFraction: Int = -1,
      workload: ReadWriteWorkload = new UniformReadWriteWorkload(1, 1, 1, 0),
      readWorkload: ReadWriteWorkload = new UniformReadWriteWorkload(1, 1, 1, 0),
      writeWorkload: ReadWriteWorkload =
        new UniformReadWriteWorkload(1, 1, 1, 0),
      // Options.
      options: ClientOptions = ClientOptions.default
  )

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def optionAction(
        f: (A, ClientOptions) => ClientOptions
    ): scopt.OptionDef[A, Flags] =
      o.action((x, flags) => flags.copy(options = f(x, flags.options)))
  }

  val parser = new scopt.OptionParser[Flags]("") {
    // Basic flags.
    opt[String]("host").required().action((x, f) => f.copy(host = x))
    opt[Int]("port").required().action((x, f) => f.copy(port = x))
    opt[File]("config").required().action((x, f) => f.copy(configFile = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"Prometheus port; -1 to disable")

    // Benchmark flags.
    opt[Int]("measurement_group_size")
      .action((x, f) => f.copy(measurementGroupSize = x))
    opt[java.time.Duration]("warmup_duration")
      .action((x, f) => f.copy(warmupDuration = x))
    opt[Duration]("warmup_timeout")
      .action((x, f) => f.copy(warmupTimeout = x))
    opt[java.time.Duration]("warmup_sleep")
      .action((x, f) => f.copy(warmupSleep = x))
    opt[Int]("num_warmup_clients")
      .action((x, f) => f.copy(numWarmupClients = x))
    opt[java.time.Duration]("duration")
      .action((x, f) => f.copy(duration = x))
    opt[Duration]("timeout")
      .action((x, f) => f.copy(timeout = x))
    opt[Int]("num_clients")
      .action((x, f) => f.copy(numClients = x))
    opt[String]("output_file_prefix")
      .action((x, f) => f.copy(outputFilePrefix = x))
    opt[ReadConsistency]("read_consistency")
      .action((x, f) => f.copy(readConsistency = x))

    // Workload flags.
    opt[Int]("predetermined_read_fraction")
      .validate(x => {
        if (-1 <= x && x <= 100) {
          Right(())
        } else {
          Left("predetermined_read_fraction must be in the range [-1, 100]")
        }
      })
      .action((x, f) => f.copy(predeterminedReadFraction = x))
    opt[ReadWriteWorkload]("workload")
      .action((x, f) => f.copy(workload = x))
    opt[ReadWriteWorkload]("read_workload")
      .action((x, f) => f.copy(readWorkload = x))
    opt[ReadWriteWorkload]("write_workload")
      .action((x, f) => f.copy(writeWorkload = x))

    // Options.
    opt[java.time.Duration]("options.resendClientRequestPeriod")
      .optionAction((x, o) => o.copy(resendClientRequestPeriod = x))
    opt[java.time.Duration]("options.resendMaxSlotRequestsPeriod")
      .optionAction((x, o) => o.copy(resendMaxSlotRequestsPeriod = x))
    opt[java.time.Duration]("options.resendReadRequestPeriod")
      .optionAction((x, o) => o.copy(resendReadRequestPeriod = x))
    opt[java.time.Duration]("options.resendSequentialReadRequestPeriod")
      .optionAction((x, o) => o.copy(resendSequentialReadRequestPeriod = x))
    opt[java.time.Duration]("options.resendEventualReadRequestPeriod")
      .optionAction((x, o) => o.copy(resendEventualReadRequestPeriod = x))
    opt[Boolean]("options.unsafeReadAtFirstSlot")
      .optionAction((x, o) => o.copy(unsafeReadAtFirstSlot = x))
    opt[Boolean]("options.unsafeReadAtI")
      .optionAction((x, o) => o.copy(unsafeReadAtI = x))
    opt[Int]("options.flushWritesEveryN")
      .optionAction((x, o) => o.copy(flushWritesEveryN = x))
    opt[Int]("options.flushReadsEveryN")
      .optionAction((x, o) => o.copy(flushReadsEveryN = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Start prometheus.
  val prometheusServer =
    PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)

  // Construct client.
  val logger = new PrintLogger(flags.logLevel)
  val transport = new NettyTcpTransport(logger)
  val client = new Client[NettyTcpTransport](
    address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port)),
    transport = transport,
    logger = logger,
    config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath()),
    options = flags.options,
    metrics = new ClientMetrics(PrometheusCollectors)
  )

  // Functions to warmup and run the clients. When warming up, we don't record
  // any stats.
  def warmupRun(
      pseudonym: Int,
      workload: ReadWriteWorkload
  ): Future[Unit] = {
    implicit val context = transport.executionContext
    val (future, error) = (workload.get(), flags.readConsistency) match {
      case (Write(command), _) =>
        (client.write(pseudonym, command), "Write failed.")
      case (Read(command), Linearizable) =>
        (client.read(pseudonym, command), "Read failed.")
      case (Read(command), Sequential) =>
        (client.sequentialRead(pseudonym, command), "Sequential read failed.")
      case (Read(command), Eventual) =>
        (client.eventualRead(pseudonym, command), "Eventual read failed.")
    }

    future.transformWith({
      case scala.util.Failure(_) =>
        logger.debug(error)
        Future.successful(())

      case scala.util.Success(_) =>
        Future.successful(())
    })
  }

  val recorder = new BenchmarkUtil.LabeledRecorder(
    s"${flags.outputFilePrefix}_data.csv",
    groupSize = flags.measurementGroupSize
  )
  def run(pseudonym: Int, workload: ReadWriteWorkload): Future[Unit] = {
    implicit val context = transport.executionContext
    val (f, error, label) = (workload.get(), flags.readConsistency) match {
      case (Write(command), _) =>
        (() => client.write(pseudonym, command), "Write failed.", "write")
      case (Read(command), Linearizable) =>
        (() => client.read(pseudonym, command), "Read failed.", "read")
      case (Read(command), Sequential) =>
        (() => client.sequentialRead(pseudonym, command),
         "Sequential read failed.",
         "read")
      case (Read(command), Eventual) =>
        (() => client.eventualRead(pseudonym, command),
         "Eventual read failed.",
         "read")
    }

    BenchmarkUtil
      .timed(f)
      .transformWith({
        case scala.util.Failure(_) =>
          logger.debug(error)
          Future.successful(())

        case scala.util.Success((_, timing)) =>
          recorder.record(
            start = timing.startTime,
            stop = timing.stopTime,
            latencyNanos = timing.durationNanos,
            label = label
          )
          Future.successful(())
      })
  }

  // Warm up the protocol.
  implicit val context = transport.executionContext
  val warmupFutures = {
    if (flags.predeterminedReadFraction == -1) {
      for (pseudonym <- 0 until flags.numWarmupClients)
        yield
          BenchmarkUtil.runFor(() => warmupRun(pseudonym, flags.workload),
                               flags.warmupDuration)
    } else {
      val readerFraction = flags.predeterminedReadFraction.toFloat / 100
      val numReaders = (readerFraction * flags.numWarmupClients).ceil.toInt
      for (pseudonym <- 0 until flags.numWarmupClients)
        yield {
          val workload = if (pseudonym < numReaders) {
            flags.readWorkload
          } else {
            flags.writeWorkload
          }
          BenchmarkUtil.runFor(() => warmupRun(pseudonym, workload),
                               flags.warmupDuration)
        }
    }
  }
  try {
    logger.info("Client warmup started.")
    concurrent.Await.result(Future.sequence(warmupFutures), flags.warmupTimeout)
    logger.info("Client warmup finished successfully.")
  } catch {
    case e: java.util.concurrent.TimeoutException =>
      logger.warn("Client warmup futures timed out!")
      logger.warn(e.toString())
  }

  // Sleep to let protocol settle.
  Thread.sleep(flags.warmupSleep.toMillis())

  // Run the benchmark.
  val futures = {
    if (flags.predeterminedReadFraction == -1) {
      for (pseudonym <- flags.numWarmupClients until
             flags.numWarmupClients + flags.numClients)
        yield
          BenchmarkUtil.runFor(() => run(pseudonym, flags.workload),
                               flags.duration)
    } else {
      val readerFraction = flags.predeterminedReadFraction.toFloat / 100
      val numReaders = (readerFraction * flags.numClients).ceil.toInt
      for (pseudonym <- flags.numWarmupClients until
             flags.numWarmupClients + flags.numClients)
        yield {
          val workload = if (pseudonym - flags.numWarmupClients < numReaders) {
            flags.readWorkload
          } else {
            flags.writeWorkload
          }
          BenchmarkUtil.runFor(() => run(pseudonym, workload), flags.duration)
        }
    }
  }
  try {
    logger.info("Clients started.")
    concurrent.Await.result(Future.sequence(futures), flags.timeout)
    logger.info("Clients finished successfully.")
  } catch {
    case e: java.util.concurrent.TimeoutException =>
      logger.warn("Client futures timed out!")
      logger.warn(e.toString())
  }
  recorder.flush()

  // Shut everything down.
  logger.info("Shutting down transport.")
  transport.shutdown()
  logger.info("Transport shut down.")

  prometheusServer.foreach(server => {
    logger.info("Stopping prometheus.")
    server.stop()
    logger.info("Prometheus stopped.")
  })
}
