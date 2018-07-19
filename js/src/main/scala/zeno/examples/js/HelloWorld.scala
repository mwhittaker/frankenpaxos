package zeno.examples.js

import scala.scalajs.js.annotation._;

@JSExportAll
@JSExportTopLevel("zeno.HelloWorld")
object HelloWorld {
  def hello(): Unit = {
    println("Hello, World!")
  }
}
