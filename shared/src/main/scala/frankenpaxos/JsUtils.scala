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
  def mapToJs[A](m: mutable.Map[String, A]): js.Dictionary[A] = m.toJSDictionary
  def optionToJs[A](o: Option[A]): js.UndefOr[A] = o.orUndefined
  def tupleToJs[A1, A2](t: (A1, A2)): js.Array[Any] =
    seqToJs(Seq[Any](t._1, t._2))
  def tupleToJs[A1, A2, A3](t: (A1, A2, A3)): js.Array[Any] =
    seqToJs(Seq[Any](t._1, t._2, t._3))
  def tupleToJs[A1, A2, A3, A4](t: (A1, A2, A3, A4)): js.Array[Any] =
    seqToJs(Seq[Any](t._1, t._2, t._3, t._4))
  def tupleToJs[A1, A2, A3, A4, A5](t: (A1, A2, A3, A4, A5)): js.Array[Any] =
    seqToJs(Seq[Any](t._1, t._2, t._3, t._4, t._5))
}
