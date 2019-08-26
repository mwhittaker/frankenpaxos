package frankenpaxos

import com.google.protobuf.ByteString

trait Serializer[A] {
  def toBytes(x: A): Array[Byte]
  def fromBytes(bytes: Array[Byte]): A
  def toPrettyString(x: A): String = ""
}

object Serializer {
  implicit val intSerializer: Serializer[Int] = new Serializer[Int] {
    override def toBytes(x: Int): Array[Byte] = SerializerInt(x).toByteArray
    override def fromBytes(bytes: Array[Byte]): Int =
      SerializerInt.parseFrom(bytes).x
    override def toPrettyString(x: Int): String = x.toString()
  }

  implicit val stringSerializer: Serializer[String] = new Serializer[String] {
    override def toBytes(x: String): Array[Byte] = x.getBytes()
    override def fromBytes(bytes: Array[Byte]): String = new String(bytes)
    override def toPrettyString(x: String): String = x
  }

  implicit val bytesSerializer: Serializer[Array[Byte]] =
    new Serializer[Array[Byte]] {
      override def toBytes(x: Array[Byte]): Array[Byte] = x
      override def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
      override def toPrettyString(x: Array[Byte]): String = x.mkString(", ")
    }

  implicit def tupleSerializer[A, B](
      implicit aSerializer: Serializer[A],
      bSerializer: Serializer[B]
  ): Serializer[(A, B)] = new Serializer[(A, B)] {
    override def toBytes(t: (A, B)): Array[Byte] = {
      val (a, b) = t
      SerializerTuple(
        a = ByteString.copyFrom(aSerializer.toBytes(a)),
        b = ByteString.copyFrom(bSerializer.toBytes(b))
      ).toByteArray
    }

    override def fromBytes(bytes: Array[Byte]): (A, B) = {
      val proto = SerializerTuple.parseFrom(bytes)
      (aSerializer.fromBytes(proto.a.toByteArray()),
       bSerializer.fromBytes(proto.b.toByteArray()))
    }

    override def toPrettyString(t: (A, B)): String = t.toString()
  }
}
