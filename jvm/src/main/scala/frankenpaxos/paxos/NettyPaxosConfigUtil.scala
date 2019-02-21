package frankenpaxos.paxos

import java.net.InetSocketAddress
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport

object NettyPaxosConfigUtil {
  def fromProto(
      proto: NettyPaxosConfigProto
  ): PaxosConfig[NettyTcpTransport] = {
    PaxosConfig(
      f = proto.f,
      proposerAddresses = proto.proposerAddress.map(
        hp => NettyTcpAddress(new InetSocketAddress(hp.host, hp.port))
      ),
      acceptorAddresses = proto.acceptorAddress.map(
        hp => NettyTcpAddress(new InetSocketAddress(hp.host, hp.port))
      )
    )
  }

  def fromFile(
      filename: String
  ): PaxosConfig[NettyTcpTransport] = {
    val source = scala.io.Source.fromFile(filename)
    try {
      fromProto(NettyPaxosConfigProto.fromAscii(source.mkString))
    } finally {
      source.close()
    }
  }
}
