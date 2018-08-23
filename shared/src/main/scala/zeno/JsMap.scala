package zeno

import scala.collection.mutable.Buffer
import scala.collection.mutable.Map
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation._

@JSExportAll
class JsMap[K, V](map: Map[K, V]) {
  def get(key: K): V = { map.get(key).get }

  def keys(): js.Array[K] = { map.keys.toJSArray }

  def items(): js.Array[js.Array[Any]] = {
    val kvs = for ((k, v) <- map) yield Seq(k, v).toJSArray
    kvs.toJSArray
  }
}
