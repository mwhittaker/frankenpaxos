package frankenpaxos.epaxos

import frankenpaxos.statemachine._
import scalax.collection.GraphEdge._
import scalax.collection.GraphPredef._
import scalax.collection.mutable.Graph

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
class DependencyGraph {

  val graph: scalax.collection.mutable.Graph[(Command, Int), DiEdge] = scalax.collection.mutable.Graph()
  var debug: String = ""

  def addNeighbors(command: (Command, Int), edges: ListBuffer[(Command, Int)]): Unit = {
    graph.add(command)
    for (edge <- edges) {
      graph += (command ~> edge)
    }
  }
  def executeGraph(stateMachine: KeyValueStore, executedCommands: mutable.Set[Command]): Unit = {
    val components = graph.strongComponentTraverser().map(_.toGraph)
    val g: Graph[Int, DiEdge] = Graph()
    val prunedGraph: Graph[(Command, Int), DiEdge] = graph.clone()

    // Map from Node to Component number
    val componentNodeSet: mutable.Map[(Command, Int), Int] = mutable.Map()

    var componentNum: Int = 0
    for (component <- components) {
      for (edge <- component.edges) {
        prunedGraph -= edge
        for (node <- edge.nodes) {
          componentNodeSet.put(node.value, componentNum)
        }
      }
      componentNum += 1
    }

    val seenSet = graph.nodes.map(_.value).toSet.diff(componentNodeSet.keys.toSet)
    val nodeIterator: Iterator[(Command, Int)] = seenSet.toIterator
    debug = seenSet.toString()
    // Map from component number to SCC graph component
    val newComponents: mutable.Map[Int, scalax.collection.Graph[(Command, Int), DiEdge]] = mutable.Map()

    for (component <- components) {
      if (component.nodes.isEmpty) {
        val node = nodeIterator.next()
        componentNodeSet.put((node._1, node._2), componentNum)
        newComponents.put(componentNum, component + node)
        componentNum += 1
      } else {
        newComponents.put(componentNodeSet.getOrElse((component.nodes.head._1, component.nodes.head._2), 0), component)
      }
    }

    //debug = newComponents.toString()

    for (integer <- newComponents.keys) {
      debug = integer.toString
      //g.add(integer)
      //g += (integer ~> integer)
    }

    debug = "Reached after graph"

    for (edge <- prunedGraph.edges) {
      val nodeIterator = edge.nodes.toIterator
      val one = nodeIterator.next()
      val two = nodeIterator.next()

      val vertexOne: Option[Int] = componentNodeSet.get((one._1, one._2))
      val vertexTwo: Option[Int] = componentNodeSet.get((two._1, two._2))

      if (vertexOne.nonEmpty && vertexTwo.nonEmpty) {
        g += (vertexOne.get ~> vertexTwo.get)
      }
    }

    for (k <- newComponents.keys) {
      g += k
    }

    debug = "Early exit"

    // Topologically sort the SCC graph
    //debug = g.nodes.toString()
    println(g.nodes.toString())
    val topSort = g.topologicalSort
    if (topSort.isRight) {
      //debug = "Made it past isRight"
      // For each strongly connected component in reverse top order
      //debug = topSort.right.get.toString()
      for (node <- topSort.right.get.toSeq.reverseIterator) {
        val nodeList: Option[scalax.collection.Graph[(Command, Int), DiEdge]] = newComponents.get(node.value)
        //debug = nodeList.get.value.toString()
        if (nodeList.nonEmpty) {
          val ordering = nodeList.get.nodes.toList.sortBy(_.value._2)
          for (value <- ordering) {
            // Execute commaand
            //stateMachine.executeCommand(value.value._1)
            //debug = value.value._1
            if (!executedCommands.contains(value.value._1)) {
              executeCommand(value.value._1.command.toStringUtf8, stateMachine)
              executedCommands.add(value.value._1)
            }
          }
        }
      }
    }
  }

  private def executeCommand(command: String, stateMachine: KeyValueStore): Unit = {
    val tokens = command.split(" ")
    if (tokens.nonEmpty) {
      tokens(0) match {
        case "GET" => {
          stateMachine.typedRun(Input().withGetRequest(GetRequest(Seq(tokens(1)))))
        }
        case "SET" => {
          stateMachine.typedRun(Input().withSetRequest(SetRequest(Seq(SetKeyValuePair(key = tokens(1), value = tokens(2))))))
        }
      }
    }
  }
}
