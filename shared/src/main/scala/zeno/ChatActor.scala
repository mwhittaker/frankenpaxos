package zeno

class ChatServerActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport
) extends Actor(address, transport) {
  override def html(): String = {
    "ChatServerActor"
  }

  override def receive(src: Transport#Address, msg: String): Unit = {
    println(s"$msg from $src.")
  }
}
