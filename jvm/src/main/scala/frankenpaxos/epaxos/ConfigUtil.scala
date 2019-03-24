package frankenpaxos.epaxos
import java.net.InetSocketAddress

import frankenpaxos.{NettyTcpAddress, NettyTcpTransport}

object ConfigUtil {
  def fromProto(proto: NettyConfigProto): Config[NettyTcpTransport] = {
    Config(
      f = proto.f,
      replicaAddresses = proto.replicaAddress.map(
        hp => NettyTcpAddress(new InetSocketAddress(hp.host, hp.port))
      )
    )
  }

  def fromFile(
      filename: String
  ): Config[NettyTcpTransport] = {
    val source = scala.io.Source.fromFile(filename)
    try {
      fromProto(NettyConfigProto.fromAscii(source.mkString))
    } finally {
      source.close()
    }
  }
}
