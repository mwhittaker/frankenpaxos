package frankenpaxos.fastmultipaxos

import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.statemachine
import frankenpaxos.statemachine.Flags.stateMachineTypeRead
import java.io.File

object LeaderMain extends App {
  case class Flags(
      index: Int = -1,
      paxosConfigFile: File = new File("."),
      stateMachineType: statemachine.StateMachineType = statemachine.TRegister
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[Int]('i', "index")
      .required()
      .valueName("<index>")
      .action((x, f) => f.copy(index = x))
      .text("Leader index")

    opt[File]('c', "config")
      .required()
      .valueName("<file>")
      .action((x, f) => f.copy(paxosConfigFile = x))
      .text("Configuration file.")

    opt[statemachine.StateMachineType]('s', "state_machine")
      .valueName(statemachine.Flags.valueName)
      .action((x, f) => f.copy(stateMachineType = x))
      .text("State machine type")
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger);
  val config = ConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
  val address = config.leaderAddresses(flags.index)
  val stateMachine = statemachine.Flags.make(flags.stateMachineType)
  new Leader[NettyTcpTransport](address,
                                transport,
                                logger,
                                config,
                                stateMachine)
}
