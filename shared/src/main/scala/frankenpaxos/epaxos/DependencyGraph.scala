package frankenpaxos.epaxos

import scalax.collection.GraphEdge._
import scalax.collection.GraphPredef._
import scalax.collection.mutable.Graph

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
class DependencyGraph {

  val graph: scalax.collection.mutable.Graph[(String, Int), DiEdge] = scalax.collection.mutable.Graph()
  var debug: String = ""

  def addNeighbors(command: (String, Int), edges: ListBuffer[(String, Int)]): Unit = {
    graph.add(command)
    for (edge <- edges) {
      graph += (command ~> edge)
    }
  }
  def executeGraph(stateMachine: StateMachine): Unit = {
    val components = graph.strongComponentTraverser().map(_.toGraph)
    val g: Graph[Int, DiEdge] = Graph()
    val prunedGraph: Graph[(String, Int), DiEdge] = graph.clone()

    // Map from Node to Component number
    val componentNodeSet: mutable.Map[(String, Int), Int] = mutable.Map()

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
    val nodeIterator: Iterator[(String, Int)] = seenSet.toIterator
    debug = seenSet.toString()
    // Map from component number to SCC graph component
    val newComponents: mutable.Map[Int, scalax.collection.Graph[(String, Int), DiEdge]] = mutable.Map()

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

    // Topologically sort the SCC graph
    val topSort = g.topologicalSort
    if (topSort.isRight) {
      // For each strongly connected component in reverse top order
      for (node <- topSort.right.get.toSeq.reverseIterator) {
        val nodeList: Option[scalax.collection.Graph[(String, Int), DiEdge]] = newComponents.get(node.value)
        if (nodeList.nonEmpty) {
          val ordering = nodeList.get.nodes.toList.sortBy(_.value._2)
          for (value <- ordering) {
            // Execute commaand
            stateMachine.executeCommand(value.value._1)
          }
        }
      }
    }
  }
}
