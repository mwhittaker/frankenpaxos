package frankenpaxos.fastmultipaxos

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
      host: String = "localhost",
      port: Int = 9000,
      paxosConfigFile: File = new File("."),
      options: ClientOptions = ClientOptions.default,
      duration: Duration = 5 seconds,
      numThreads: Int = 1,
      outputFilePrefix: String = ""
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

    opt[Duration]("repropose_period")
      .valueName("<repropose_period>")
      .action((x, f) => {
        val d = java.time.Duration.ofNanos(x.toNanos)
        val options = f.options.copy(reproposePeriod = d)
        f.copy(options = options)
      })
      .text("Configuration file.")

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

  val startTime = java.time.Instant.now()
  val stopTime =
    startTime.plus(java.time.Duration.ofNanos(flags.duration.toNanos))
  val threads = for (i <- 0 until flags.numThreads) yield {
    val thread = new Thread {
      override def run() {
        val logger = new FileLogger(s"${flags.outputFilePrefix}_$i.txt")
        logger.debug(s"Client $i started.")
        val transport = new NettyTcpTransport(logger);
        val address = NettyTcpAddress(
          new InetSocketAddress(flags.host, flags.port + i)
        )
        val config =
          ConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
        val latency_writer =
          CSVWriter.open(new File(s"${flags.outputFilePrefix}_$i.csv"))
        latency_writer.writeRow(
          Seq("host", "port", "start", "stop", "latency_nanos")
        )
        val paxosClient =
          new Client[NettyTcpTransport](address, transport, logger, config)

        while (java.time.Instant.now().isBefore(stopTime)) {
          // Note that this client will only work for some state machine (e.g.,
          // Register and AppendLog) and won't work for others (e.g.,
          // KeyValueStore).
          val cmdStart = java.time.Instant.now()
          val cmdStartNanos = System.nanoTime()
          concurrent.Await
            .result(paxosClient.propose("."), concurrent.duration.Duration.Inf)
          val cmdStopNanos = System.nanoTime()
          val cmdStop = java.time.Instant.now()
          latency_writer.writeRow(
            Seq(
              flags.host,
              (flags.port + i).toString(),
              cmdStart.toString(),
              cmdStop.toString(),
              (cmdStopNanos - cmdStartNanos).toString()
            )
          )
        }
        transport.shutdown().await()
      }
    }
    thread.start()
    // Sleep slightly to stagger clients.
    Thread.sleep(100 /*ms*/ )
    thread
  }

  for (thread <- threads) {
    thread.join()
  }
}
