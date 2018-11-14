package zeno

import scala.collection.mutable
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation._

@JSExportAll
@JSExportTopLevel("zeno.JsUtils")
object JsUtils {
  def seqToJs[A](s: mutable.Seq[A]): js.Array[A] = s.toJSArray
  def mapToJs[A](m: mutable.Map[String, A]): js.Dictionary[A] = m.toJSDictionary
  def optionToJs[A](o: Option[A]): js.UndefOr[A] = o.orUndefined
}
