package frankenpaxos.fastmultipaxos

import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import java.io.File
import scala.concurrent.duration

object AcceptorMain extends App {
  case class Flags(
      // Basic flags.
      index: Int = -1,
      paxosConfigFile: File = new File("."),
      // Options.
      options: AcceptorOptions = AcceptorOptions.default
  )

  val parser = new scopt.OptionParser[Flags]("") {
    // Basic flags.
    opt[Int]('i', "index")
      .required()
      .valueName("<index>")
      .action((x, f) => f.copy(index = x))
      .text("Acceptor index")

    opt[File]('c', "config")
      .required()
      .valueName("<file>")
      .action((x, f) => f.copy(paxosConfigFile = x))
      .text("Configuration file.")

    // Options.
    opt[duration.Duration]("options.waitPeriod")
      .action((x, f) => {
        f.copy(
          options = f.options
            .copy(waitPeriod = java.time.Duration.ofNanos(x.toNanos))
        )
      })

    opt[duration.Duration]("options.waitStagger")
      .action((x, f) => {
        f.copy(
          options = f.options
            .copy(waitStagger = java.time.Duration.ofNanos(x.toNanos))
        )
      })

  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger);
  val config = ConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
  val address = config.acceptorAddresses(flags.index)
  new Acceptor[NettyTcpTransport](address,
                                  transport,
                                  logger,
                                  config,
                                  flags.options)
}
