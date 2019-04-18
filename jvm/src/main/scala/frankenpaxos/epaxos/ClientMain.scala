package frankenpaxos.epaxos

import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.io.Source

object ClientMain extends App {
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
  val address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port))
  val config = ConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
  val paxosClient =
    new Client[NettyTcpTransport](address, transport, logger, config)
  val commandsToRun = Source
    .fromFile(
      "/Users/neil/Documents/Databases/frankenpaxos/zeno/jvm/src/main/scala/frankenpaxos/epaxos/commands.txt"
    )
    .getLines()
    .toList

  val start_time = java.time.Instant.now()
  for (value <- commandsToRun) {
    //val value = readLine()
    val future = paxosClient.propose(value)
    println(concurrent.Await.result(future, concurrent.duration.Duration.Inf))
    //paxosClient
    //  .propose(value)
    //.foreach(value => println(s"$value was chosen."))(
    //  scala.concurrent.ExecutionContext.global
    //)
  }
  val stop_time = java.time.Instant.now()
  val duration = java.time.Duration.between(start_time, stop_time)
  println("Client throughput is: " + duration.toMillis)
}
