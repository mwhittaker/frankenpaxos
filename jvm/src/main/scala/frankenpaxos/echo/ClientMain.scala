package frankenpaxos.echo

import java.net.InetAddress
import java.net.InetSocketAddress
import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger

object ClientMain extends App {
  case class Flags(
      serverHost: String = "localhost",
      serverPort: Int = 9000,
      host: String = "localhost",
      port: Int = 9001
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
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger)
  val srcAddress = NettyTcpAddress(
    new InetSocketAddress(flags.host, flags.port)
  )
  val dstAddress = NettyTcpAddress(
    new InetSocketAddress(flags.serverHost, flags.serverPort)
  )
  val chatClient =
    new Client[NettyTcpTransport](srcAddress, dstAddress, transport, logger)
  var ok = true
  while (ok) {
    val line = readLine()
    ok = line != null
    if (ok) {
      chatClient.echo(line)
    }
  }
}
