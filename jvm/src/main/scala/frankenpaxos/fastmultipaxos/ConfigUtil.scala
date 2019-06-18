package frankenpaxos.fastmultipaxos

import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.roundsystem.RoundSystem
import java.net.InetSocketAddress

object ConfigUtil {
  def fromProto(proto: ConfigProto): Config[NettyTcpTransport] = {
    val toAddresses = (hostports: Seq[HostPortProto]) =>
      hostports.map(
        hp => NettyTcpAddress(new InetSocketAddress(hp.host, hp.port))
      )

    Config[NettyTcpTransport](
      f = proto.f,
      leaderAddresses = toAddresses(proto.leaderAddress),
      leaderElectionAddresses = toAddresses(proto.leaderElectionAddress),
      leaderHeartbeatAddresses = toAddresses(proto.leaderHeartbeatAddress),
      acceptorAddresses = toAddresses(proto.acceptorAddress),
      acceptorHeartbeatAddresses = toAddresses(proto.acceptorHeartbeatAddress),
      roundSystem = proto.roundSystemType match {
        case RoundSystemType.CLASSIC_ROUND_ROBIN =>
          new RoundSystem.ClassicRoundRobin(proto.leaderAddress.size)
        case RoundSystemType.ROUND_ZERO_FAST =>
          new RoundSystem.RoundZeroFast(proto.leaderAddress.size)
        case RoundSystemType.MIXED_ROUND_ROBIN =>
          new RoundSystem.MixedRoundRobin(proto.leaderAddress.size)
        case RoundSystemType.Unrecognized(_) => ???
      }
    )
  }

  def fromFile(filename: String): Config[NettyTcpTransport] = {
    val source = scala.io.Source.fromFile(filename)
    try {
      fromProto(ConfigProto.fromAscii(source.mkString))
    } finally {
      source.close()
    }
  }
}
