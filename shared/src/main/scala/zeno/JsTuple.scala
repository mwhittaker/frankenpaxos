package zeno

import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation._

@JSExportAll
class JsTuple2[A, B](tuple: (A, B)) {
  def toJs(): js.Array[Any] = {
    val (a, b) = tuple
    Seq(a, b).toJSArray
  }
}
