package zeno

trait Serializer[A] {
  def toBytes(x: A): Array[Byte]
  def fromBytes(bytes: Array[Byte]): A
  def toPrettyString(x: A): String = { "" }
}
