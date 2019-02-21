package frankenpaxos.paxos.jvm

import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.paxos.PaxosClientActor

object PaxosClientMain extends App {
  case class Flags(
      host: String = "localhost",
      port: Int = 9000,
      paxosConfigFile: File = new File(".")
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[String]('h', "host")
      .valueName("<host>")
      .action((x, f) => f.copy(host = x))
      .text("Client hostname.")

    opt[Int]('p', "port")
      .valueName("<port>")
      .action((x, f) => f.copy(port = x))
      .text("Client port")

    opt[File]('c', "config")
      .required()
      .valueName("<file>")
      .action((x, f) => f.copy(paxosConfigFile = x))
      .text("Paxos configuration file.")
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
  val address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port));
  val config =
    NettyPaxosConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
  val paxosClient =
    new PaxosClientActor[NettyTcpTransport](address, transport, logger, config);

  while (true) {
    val value = readLine()
    paxosClient
      .propose(value)
      .foreach(value => println(s"$value was chosen."))(
        scala.concurrent.ExecutionContext.global
      )
  }
}
