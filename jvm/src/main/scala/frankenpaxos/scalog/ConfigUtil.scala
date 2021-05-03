package frankenpaxos.scalog

import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import java.net.InetSocketAddress

object ConfigUtil {
  def fromProto(proto: NettyConfigProto): Config[NettyTcpTransport] = {
    def addr(hostPort: HostPortProto): NettyTcpAddress =
      NettyTcpAddress(new InetSocketAddress(hostPort.host, hostPort.port))

    Config[NettyTcpTransport](
      f = proto.f,
      serverAddresses =
        proto.serverAddress.map(shard => shard.serverAddress.map(addr)),
      aggregatorAddress = addr(proto.aggregatorAddress),
      leaderAddresses = proto.leaderAddress.map(addr),
      leaderElectionAddresses = proto.leaderElectionAddress.map(addr),
      acceptorAddresses = proto.acceptorAddress.map(addr),
      replicaAddresses = proto.replicaAddress.map(addr),
      proxyReplicaAddresses = proto.proxyReplicaAddress.map(addr)
    )
  }

  def fromFile(filename: String): Config[NettyTcpTransport] = {
    val source = scala.io.Source.fromFile(filename)
    try {
      fromProto(NettyConfigProto.fromAscii(source.mkString))
    } finally {
      source.close()
    }
  }
}
