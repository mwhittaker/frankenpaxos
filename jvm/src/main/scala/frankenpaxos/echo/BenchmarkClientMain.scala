package frankenpaxos.echo

import collection.mutable
import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.FileLogger
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.Future
import scala.util.Try

object BenchmarkClientMain extends App {
  case class Flags(
      host: String = "localhost",
      port: Int = 9001,
      serverHost: String = "localhost",
      serverPort: Int = 9000,
      duration: java.time.Duration = java.time.Duration.ofSeconds(0),
      numClients: Int = 1,
      outputFile: String = ""
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[String]("host").action((x, f) => f.copy(host = x))
    opt[Int]("port").action((x, f) => f.copy(port = x))
    opt[String]("server_host").action((x, f) => f.copy(serverHost = x))
    opt[Int]("server_port").action((x, f) => f.copy(serverPort = x))
    opt[scala.concurrent.duration.Duration]("duration")
      .action(
        (x, f) => f.copy(duration = java.time.Duration.ofNanos(x.toNanos))
      )
    opt[Int]("num_clients").action((x, f) => f.copy(numClients = x))
    opt[String]("output_file").action((x, f) => f.copy(outputFile = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger)
  val srcAddress = NettyTcpAddress(
    new InetSocketAddress(flags.host, flags.port)
  )
  val dstAddress = NettyTcpAddress(
    new InetSocketAddress(flags.serverHost, flags.serverPort)
  )
  val client = new BenchmarkClient[NettyTcpTransport](srcAddress,
                                                      dstAddress,
                                                      transport,
                                                      logger)
  val latencyWriter = CSVWriter.open(new File(flags.outputFile))
  latencyWriter.writeRow(Seq("start", "stop", "latency_nanos", "address"))
  val benchmarkStopTime = java.time.Instant.now().plus(flags.duration)

  def f(startTime: java.time.Instant, startTimeNanos: Long)(
      reply: Try[Unit]
  ): Future[Unit] = {
    reply match {
      case scala.util.Failure(_) =>
        logger.debug("Echo failed.")

      case scala.util.Success(_) =>
        val stopTimeNanos = System.nanoTime()
        val stopTime = java.time.Instant.now()
        latencyWriter.writeRow(
          Seq(
            startTime.toString(),
            stopTime.toString(),
            (stopTimeNanos - startTimeNanos).toString(),
            srcAddress.toString()
          )
        )
    }

    if (java.time.Instant.now().isBefore(benchmarkStopTime)) {
      client
        .echo()
        .transformWith(f(java.time.Instant.now(), System.nanoTime()))(
          transport.executionContext
        )
    } else {
      Future.successful(())
    }
  }

  val futures = for (i <- 0 until flags.numClients) yield {
    client
      .echo()
      .transformWith(f(java.time.Instant.now(), System.nanoTime()))(
        transport.executionContext
      )
  }
  concurrent.Await.result(
    Future.sequence(futures)(implicitly, transport.executionContext),
    scala.concurrent.duration.Duration.Inf
  )
  logger.debug("Shutting down transport.")
  transport.shutdown()
  logger.debug("Transport shut down.")
}
