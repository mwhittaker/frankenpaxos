package frankenpaxos

import scala.collection.mutable
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation._

@JSExportAll
@JSExportTopLevel("frankenpaxos.JsUtils")
object JsUtils {
  def seqToJs[A](s: Seq[A]): js.Array[A] = s.toJSArray
  def seqToJs[A](s: mutable.Seq[A]): js.Array[A] = s.toJSArray

  def setToJs[A](m: Set[A]): js.Array[A] = seqToJs(m.to[Seq])
  def setToJs[A](m: mutable.Set[A]): js.Array[A] = seqToJs(m.to[mutable.Seq])
  def setToJs[A](m: scala.collection.SortedSet[A]): js.Array[A] =
    seqToJs(m.to[Seq])
  def setToJs[A](m: mutable.SortedSet[A]): js.Array[A] =
    seqToJs(m.to[mutable.Seq])

  def mapToJs[K, V](m: Map[K, V]): js.Array[js.Array[Any]] = {
    seqToJs(m.map({ case (k, v) => tupleToJs((k, v)) }).to[Seq])
  }
  def mapToJs[K, V](m: mutable.Map[K, V]): js.Array[js.Array[Any]] = {
    seqToJs(m.map({ case (k, v) => tupleToJs((k, v)) }).to[mutable.Seq])
  }

  def optionToJs[A](o: Option[A]): js.UndefOr[A] = o.orUndefined

  def tupleToJs[A1, A2](t: (A1, A2)): js.Array[Any] =
    seqToJs(Seq[Any](t._1, t._2))
  def tupleToJs[A1, A2, A3](t: (A1, A2, A3)): js.Array[Any] =
    seqToJs(Seq[Any](t._1, t._2, t._3))
  def tupleToJs[A1, A2, A3, A4](t: (A1, A2, A3, A4)): js.Array[Any] =
    seqToJs(Seq[Any](t._1, t._2, t._3, t._4))
  def tupleToJs[A1, A2, A3, A4, A5](t: (A1, A2, A3, A4, A5)): js.Array[Any] =
    seqToJs(Seq[Any](t._1, t._2, t._3, t._4, t._5))

  def iterableToJs[A](x: Seq[A]): js.Array[A] = seqToJs(x)
  def iterableToJs[A](x: mutable.Seq[A]): js.Array[A] = seqToJs(x)
  def iterableToJs[A](x: Set[A]): js.Array[A] = setToJs(x)
  def iterableToJs[A](x: mutable.Set[A]): js.Array[A] = setToJs(x)
  def iterableToJs[K, V](x: Map[K, V]): js.Array[js.Array[Any]] = mapToJs(x)
  def iterableToJs[K, V](x: mutable.Map[K, V]): js.Array[js.Array[Any]] =
    mapToJs(x)
  def iterableToJs[A1, A2](x: (A1, A2)): js.Array[Any] = tupleToJs(x)
  def iterableToJs[A1, A2, A3](x: (A1, A2, A3)): js.Array[Any] = tupleToJs(x)
  def iterableToJs[A1, A2, A3, A4](x: (A1, A2, A3, A4)): js.Array[Any] =
    tupleToJs(x)
  def iterableToJs[A1, A2, A3, A4, A5](x: (A1, A2, A3, A4, A5)): js.Array[Any] =
    tupleToJs(x)
}
