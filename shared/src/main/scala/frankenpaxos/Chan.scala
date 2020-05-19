package frankenpaxos

class Chan[
    Transport <: frankenpaxos.Transport[Transport],
    DstActor <: frankenpaxos.Actor[Transport]
](
    val transport: Transport,
    val srcActor: frankenpaxos.Actor[Transport],
    val dst: Transport#Address,
    val serializer: Serializer[DstActor#InboundMessage]
) {
  def send(msg: DstActor#InboundMessage): Unit =
    transport.send(srcActor, dst, serializer.toBytes(msg))
  def sendNoFlush(msg: DstActor#InboundMessage): Unit =
    transport.sendNoFlush(srcActor, dst, serializer.toBytes(msg))
  def flush(): Unit = transport.flush(srcActor, dst)
}
