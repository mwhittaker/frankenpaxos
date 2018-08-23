package zeno

import scala.collection.mutable.Buffer
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation._

@JSExportAll
class JsBuffer[A](buffer: Buffer[A]) {
  def toJs(): js.Array[A] = { buffer.toJSArray }
}
