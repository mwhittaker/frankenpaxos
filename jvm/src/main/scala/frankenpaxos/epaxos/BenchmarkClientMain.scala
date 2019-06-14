package frankenpaxos.epaxos

import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.FileLogger
import frankenpaxos.Flags.durationRead
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

object BenchmarkClientMain extends App {

  case class Flags(
      // Basic flags.
      host: String = "localhost",
      port: Int = 9000,
      paxosConfig: File = new File("."),
      duration: Duration = 5 seconds,
      outputFilePrefix: String = "",
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
    opt[String]('h', "host")
      .required()
      .action((x, f) => f.copy(host = x))

    opt[Int]('p', "port")
      .required()
      .action((x, f) => f.copy(port = x))

    opt[File]('c', "config")
      .required()
      .action((x, f) => f.copy(paxosConfig = x))

    opt[Duration]('d', "duration")
      .action((x, f) => f.copy(duration = x))

    opt[String]('o', "output_file_prefix")
      .action((x, f) => f.copy(outputFilePrefix = x))

    opt[java.time.Duration]("options.repropose_period")
      .optionAction((x, o) => o.copy(reproposePeriod = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  // val startTime = java.time.Instant.now()
  // val stopTime =
  //   startTime.plus(java.time.Duration.ofNanos(flags.duration.toNanos))
  // val threads = for (i <- 0 until flags.numThreads) yield {
  //   val thread = new Thread {
  //     override def run() {
  //       val logger = new FileLogger(s"${flags.outputFilePrefix}_$i.txt")
  //       logger.debug(s"Client $i started.")
  //       val transport = new NettyTcpTransport(logger);
  //       val address = NettyTcpAddress(
  //         new InetSocketAddress(flags.host, flags.port + i)
  //       )
  //       val config =
  //         ConfigUtil.fromFile(flags.paxosConfig.getAbsolutePath())
  //       val latency_writer =
  //         CSVWriter.open(new File(s"${flags.outputFilePrefix}_$i.csv"))
  //       latency_writer.writeRow(
  //         Seq("host", "port", "start", "stop", "latency_nanos")
  //       )
  //       val paxosClient =
  //         new Client[NettyTcpTransport](address, transport, logger, config)
  //
  //       while (java.time.Instant.now().isBefore(stopTime)) {
  //         // Note that this client will only work for some state machine (e.g.,
  //         // Register and AppendLog) and won't work for others (e.g.,
  //         // KeyValueStore).
  //         val cmdStart = java.time.Instant.now()
  //         val cmdStartNanos = System.nanoTime()
  //         concurrent.Await
  //           .result(paxosClient.propose("."), concurrent.duration.Duration.Inf)
  //         val cmdStopNanos = System.nanoTime()
  //         val cmdStop = java.time.Instant.now()
  //         latency_writer.writeRow(
  //           Seq(
  //             flags.host,
  //             (flags.port + i).toString(),
  //             cmdStart.toString(),
  //             cmdStop.toString(),
  //             (cmdStopNanos - cmdStartNanos).toString()
  //           )
  //         )
  //       }
  //       transport.shutdown()
  //     }
  //   }
  //   thread.start()
  //   // Sleep slightly to stagger clients.
  //   Thread.sleep(100 /*ms*/ )
  //   thread
  // }
  //
  // for (thread <- threads) {
  //   thread.join()
  // }
}
