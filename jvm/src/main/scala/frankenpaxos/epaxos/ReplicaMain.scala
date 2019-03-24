package frankenpaxos.epaxos

import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger

object ReplicaMain extends App {
  case class Flags(
      index: Int = -1,
      paxosConfigFile: File = new File(".")
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[Int]('i', "index")
      .required()
      .valueName("<index>")
      .action((x, f) => f.copy(index = x))
      .text("Replica index.")

    opt[File]('c', "config")
      .required()
      .valueName("<file>")
      .action((x, a) => a.copy(paxosConfigFile = x))
      .text("Configuration file.")
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      System.exit(-1)
      ???
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger);
  val config = ConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
  val address = config.replicaAddresses(flags.index)
  new Replica[NettyTcpTransport](address, transport, logger, config)
}
