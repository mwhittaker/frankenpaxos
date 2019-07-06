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
      port: Int = 10000
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[String]("server_host").action((x, f) => f.copy(serverHost = x))
    opt[Int]("server_port").action((x, f) => f.copy(serverPort = x))
    opt[String]("host").action((x, f) => f.copy(host = x))
    opt[Int]("port").action((x, f) => f.copy(port = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  val logger = new PrintLogger()
  val chatClient = new Client[NettyTcpTransport](
    srcAddress = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port)),
    dstAddress = NettyTcpAddress(
      new InetSocketAddress(flags.serverHost, flags.serverPort)
    ),
    transport = new NettyTcpTransport(logger),
    logger
  )

  var ok = true
  while (ok) {
    val line = scala.io.StdIn.readLine()
    ok = line != null
    if (ok) {
      chatClient.echo(line)
    }
  }
}
