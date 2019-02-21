package frankenpaxos

class TypedActorClient[
    Transport <: frankenpaxos.Transport[Transport],
    Actor <: frankenpaxos.Actor[Transport]
](
    val transport: Transport,
    val src: Transport#Address,
    val dst: Transport#Address,
    val serializer: Serializer[Actor#InboundMessage]
) {
  def send(msg: Actor#InboundMessage): Unit = {
    transport.send(src, dst, serializer.toBytes(msg))
  }
}
