package frankenpaxos.vue_test

import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
class VueTest {
  case class Wrapper(x: String)

  class Cell(var x: String) {
    override def toString(): String = x
  }

  var mutableString = mutable.Map[String, String]()
  var mutableCell = mutable.Map[String, Cell]()
  var mutableWrapper = mutable.Map[String, Wrapper]()

  def directAddMutableString(key: String, value: String) =
    mutableString(key) = value
  def putMutableString(key: String, value: String) =
    mutableString.put(key, value)
  def updateMutableString(key: String, value: String) =
    mutableString.update(key, value)
  def plusEqualMutableString(key: String, value: String) =
    mutableString += key -> value
  def reassignMutableString(key: String, value: String) =
    mutableString = mutableString += (key -> value)
  def plusMutableString(key: String, value: String) =
    mutableString = mutableString + (key -> value)
  def removeMutableString(key: String) =
    mutableString.remove(key)
  def minusEqualsMutableString(key: String) =
    mutableString -= key
  def minusMutableString(key: String) =
    mutableString = mutableString - key

  def directAddMutableCell(key: String, value: String) =
    mutableCell(key) = new Cell(value)
  def putMutableCell(key: String, value: String) =
    mutableCell.put(key, new Cell(value))
  def updateMutableCell(key: String, value: String) =
    mutableCell.update(key, new Cell(value))
  def plusEqualMutableCell(key: String, value: String) =
    mutableCell += key -> new Cell(value)
  def reassignMutableCell(key: String, value: String) =
    mutableCell = mutableCell += (key -> new Cell(value))
  def plusMutableCell(key: String, value: String) =
    mutableCell = mutableCell + (key -> new Cell(value))
  def mutateMutableCell(key: String, value: String) =
    mutableCell(key).x = value
  def removeMutableCell(key: String) =
    mutableCell.remove(key)
  def minusEqualsMutableCell(key: String) =
    mutableCell -= key
  def minusMutableCell(key: String) =
    mutableCell = mutableCell - key

  def directAddMutableWrapper(key: String, value: String) =
    mutableWrapper(key) = Wrapper(value)
  def putMutableWrapper(key: String, value: String) =
    mutableWrapper.put(key, Wrapper(value))
  def updateMutableWrapper(key: String, value: String) =
    mutableWrapper.update(key, Wrapper(value))
  def plusEqualMutableWrapper(key: String, value: String) =
    mutableWrapper += key -> Wrapper(value)
  def reassignMutableWrapper(key: String, value: String) =
    mutableWrapper = mutableWrapper += (key -> Wrapper(value))
  def plusMutableWrapper(key: String, value: String) =
    mutableWrapper = mutableWrapper + (key -> Wrapper(value))
  def removeMutableWrapper(key: String) =
    mutableWrapper.remove(key)
  def minusEqualsMutableWrapper(key: String) =
    mutableWrapper -= key
  def minusMutableWrapper(key: String) =
    mutableWrapper = mutableWrapper - key
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.vue_test.TweenedVueTest")
object TweenedVueTest {
  val VueTest = new VueTest();
}
