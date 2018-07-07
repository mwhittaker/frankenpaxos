package zeno

import java.net.InetAddress;
import java.net.InetSocketAddress;

class ChatServerActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport
) extends Actor(address, transport) {
  override def html(): String = {
    "ChatServerActor"
  }

  override def receive(src: Transport#Address, msg: String): Unit = {
    println(s"${msg.size} from $src.");
    send(src, msg);
  }
}

object ChatServerMain {
  def main(args: Array[String]): Unit = {
    val transport = new NettyTcpTransport();
    val address = NettyTcpAddress(
      new InetSocketAddress(InetAddress.getLocalHost(), 9000)
    );
    val chatServer = new ChatServerActor[NettyTcpTransport](address, transport);
    transport.run();
  }
}
