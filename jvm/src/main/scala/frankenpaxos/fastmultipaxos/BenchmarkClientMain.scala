package frankenpaxos.fastmultipaxos

import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.duration._

object BenchmarkClientMain extends App {
  case class Flags(
      host: String = "localhost",
      port: Int = 9000,
      paxosConfigFile: File = new File("."),
      duration: Duration = 5 seconds
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[String]('h', "host")
      .valueName("<host>")
      .action((x, f) => f.copy(host = x))
      .text("Client hostname")

    opt[Int]('p', "port")
      .valueName("<port>")
      .action((x, f) => f.copy(port = x))
      .text("Client port")

    opt[File]('c', "config")
      .required()
      .valueName("<file>")
      .action((x, f) => f.copy(paxosConfigFile = x))
      .text("Configuration file.")

    opt[Duration]('d', "duration")
      .valueName("<duration>")
      .action((x, f) => f.copy(duration = x))
      .text("Benchmark duration")
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger);
  val address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port))
  val config = ConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
  val paxosClient =
    new Client[NettyTcpTransport](address, transport, logger, config)

  val start = java.time.Instant.now()
  val stop = start.plus(java.time.Duration.ofNanos(flags.duration.toNanos))
  while (java.time.Instant.now().isBefore(stop)) {
    // Note that this client will only work for some state machine (e.g.,
    // Register and AppendLog) and won't work for others (e.g., KeyValueStore).
    val future = paxosClient.propose("foo")
    concurrent.Await.result(future, concurrent.duration.Duration.Inf)
  }
  transport.shutdown()
}
