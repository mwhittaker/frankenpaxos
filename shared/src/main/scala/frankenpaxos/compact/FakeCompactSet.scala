package frankenpaxos.compact

import scala.collection.mutable
import scala.scalajs.js.annotation._

// A FakeCompactSet[A] is really just a Set[A]. It doesn't do any form of
// compaction at all. FakeCompactSet is useful for testing.
@JSExportAll
class FakeCompactSet[A](initialValues: Set[A] = Set[A]())
    extends CompactSet[FakeCompactSet[A]] {
  override type T = A
  private val values: mutable.Set[A] = mutable.Set[A]() ++ initialValues

  override def equals(other: Any): Boolean = {
    other match {
      case other: FakeCompactSet[A] => values == other.values
      case _                        => false
    }
  }
  override def hashCode: Int = values.hashCode
  override def add(value: A): Boolean = values.add(value)
  override def contains(value: A): Boolean = values.contains(value)
  override def union(other: FakeCompactSet[A]): FakeCompactSet[A] =
    new FakeCompactSet[A](values.union(other.values).toSet)
  override def diff(other: FakeCompactSet[A]): FakeCompactSet[A] =
    new FakeCompactSet[A](values.diff(other.values).toSet)
  override def addAll(other: FakeCompactSet[A]): this.type = {
    values ++= other.values
    this
  }
  override def subtractAll(other: FakeCompactSet[A]): this.type = {
    values ++= other.values
    this
  }
  override def size: Int = values.size
  override def uncompactedSize: Int = values.size
  override def subset(): FakeCompactSet[A] = new FakeCompactSet(values.toSet)
  override def materialize(): Set[A] = values.toSet
}

object FakeCompactSet {
  def factory[A] = new CompactSetFactory[FakeCompactSet[A], A] {
    override def empty = new FakeCompactSet[A]()
    override def fromSet(xs: Set[A]) = new FakeCompactSet[A](xs)
  }
}
