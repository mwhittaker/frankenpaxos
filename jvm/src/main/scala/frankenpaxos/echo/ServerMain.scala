package frankenpaxos.echo

import java.net.InetAddress
import java.net.InetSocketAddress
import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger

object ServerMain extends App {
  case class Flags(
      host: String = "localhost",
      port: Int = 9000
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[String]("host")
      .valueName("<host>")
      .action((x, f) => f.copy(host = x))
      .text("Hostname")

    opt[Int]("port")
      .valueName("<port>")
      .action((x, f) => f.copy(port = x))
      .text("Port")
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger)
  val address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port))
  val chatServer = new Server[NettyTcpTransport](address, transport, logger)
}
