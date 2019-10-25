package frankenpaxos.spaxosdecouple

import java.net.InetSocketAddress

import frankenpaxos.{NettyTcpAddress, NettyTcpTransport}

object ConfigUtil {
  def fromProto(proto: NettyConfigProto): Config[NettyTcpTransport] = {
    def addr(hostPort: HostPortProto): NettyTcpAddress =
      NettyTcpAddress(new InetSocketAddress(hostPort.host, hostPort.port))

    Config(
      f = proto.f,
      batcherAddresses = proto.batcherAddress.map(addr),
      proposerAddresses = proto.proposerAddress.map(addr),
      executorAddresses = proto.executorAddress.map(addr),
      leaderAddresses = proto.leaderAddress.map(addr),
      leaderElectionAddresses = proto.leaderElectionAddress.map(addr),
      proxyLeaderAddresses = proto.proxyLeaderAddress.map(addr),
      acceptorAddresses =
        proto.acceptorAddress.map(group => group.acceptorAddress.map(addr)),
      replicaAddresses = proto.replicaAddress.map(addr),
      proxyReplicaAddresses = proto.proxyReplicaAddress.map(addr),
      distributionScheme = proto.distributionScheme match {
        case DistributionSchemeProto.HASH      => Hash
        case DistributionSchemeProto.COLOCATED => Colocated
        case DistributionSchemeProto.Unrecognized(_) =>
          throw new IllegalArgumentException(
            "Unrecognized DistributionSchemeProto."
          )
      }
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
