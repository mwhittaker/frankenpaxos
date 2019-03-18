package frankenpaxos.echo

import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.FileLogger
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.duration._

object BenchmarkClientMain extends App {
  case class Flags(
      serverHost: String = "localhost",
      serverPort: Int = 9000,
      host: String = "localhost",
      port: Int = 9001,
      duration: Duration = 5 seconds,
      numThreads: Int = 1,
      outputFilePrefix: String = ""
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[String]("server_host")
      .valueName("<host>")
      .action((x, f) => f.copy(serverHost = x))
      .text("Server hostname")

    opt[Int]("server_port")
      .valueName("<port>")
      .action((x, f) => f.copy(serverPort = x))
      .text("Server port")

    opt[String]("host")
      .valueName("<host>")
      .action((x, f) => f.copy(host = x))
      .text("Client hostname")

    opt[Int]("port")
      .valueName("<port>")
      .action((x, f) => f.copy(port = x))
      .text("Client port")

    opt[Duration]('d', "duration")
      .valueName("<duration>")
      .action((x, f) => f.copy(duration = x))
      .text("Benchmark duration")

    opt[Int]('t', "num_threads")
      .valueName("<num threads>")
      .action((x, f) => f.copy(numThreads = x))
      .text("Number of client threads")

    opt[String]('o', "output_file_prefix")
      .valueName("<output file prefix>")
      .action((x, f) => f.copy(outputFilePrefix = x))
      .text("Output file prefix")
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  // val logger = new PrintLogger()
  // val transport = new NettyTcpTransport(logger)
  // val chatClient =
  //   new Client[NettyTcpTransport](srcAddress, dstAddress, transport, logger)
  // var ok = true
  // while (ok) {
  //   val line = readLine()
  //   ok = line != null
  //   if (ok) {
  //     chatClient.echo(line)
  //   }
  // }

  val start_time = java.time.Instant.now()
  val stop_time =
    start_time.plus(java.time.Duration.ofNanos(flags.duration.toNanos))
  val threads = for (i <- 0 until flags.numThreads) yield {
    val thread = new Thread {
      override def run() {
        val logger = new FileLogger(s"${flags.outputFilePrefix}_$i.txt")
        logger.debug(s"Client $i started.")
        val transport = new NettyTcpTransport(logger)
        val srcAddress = NettyTcpAddress(
          new InetSocketAddress(flags.host, flags.port + i)
        )
        val dstAddress = NettyTcpAddress(
          new InetSocketAddress(flags.serverHost, flags.serverPort)
        )
        val latency_writer =
          CSVWriter.open(new File(s"${flags.outputFilePrefix}_$i.csv"))
        latency_writer.writeRow(
          Seq("host", "port", "start", "stop", "latency_nanos")
        )
        val client = new BenchmarkClient[NettyTcpTransport](srcAddress,
                                                            dstAddress,
                                                            transport,
                                                            logger)

        while (java.time.Instant.now().isBefore(stop_time)) {
          // Note that this client will only work for some state machine (e.g.,
          // Register and AppendLog) and won't work for others (e.g.,
          // KeyValueStore).
          val cmd_start = java.time.Instant.now()
          val future = client.echo(".")
          concurrent.Await.result(future, concurrent.duration.Duration.Inf)
          val cmd_stop = java.time.Instant.now()
          val latency = java.time.Duration.between(cmd_start, cmd_stop)
          latency_writer.writeRow(
            Seq(
              flags.host,
              (flags.port + i).toString(),
              cmd_start.toString(),
              cmd_stop.toString(),
              latency.toNanos().toString()
            )
          )
        }
        transport.shutdown()
      }
    }
    thread.start()
    thread
  }

  for (thread <- threads) {
    thread.join()
  }
}
