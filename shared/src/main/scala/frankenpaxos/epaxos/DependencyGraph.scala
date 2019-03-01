package frankenpaxos.epaxos

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DependencyGraph {
  class Vertex(_contents: String, _sequenceNum: Int, _index: Int, _lowLink: Int, _onStack: Boolean) {
    var contents: String = _contents
    var sequenceNum: Int = _sequenceNum
    var index: Int = _index
    var lowLink: Int = _lowLink
    var onStack: Boolean = _onStack

    override def toString: String = contents
  }

  val adjacencyList: mutable.Map[Vertex, ListBuffer[Vertex]] = mutable.Map()

  def addNeighbors(command: (String, Int), edges: ListBuffer[(String, Int)]): Unit = {
    val commandVertex: Vertex = new Vertex(command._1, command._2, -1, -1, false)
    val edgeList: ListBuffer[Vertex] = ListBuffer()
    for (edge <- edges) {
      val vertex: Vertex = new Vertex(edge._1, edge._2, -1, -1, false)
      edgeList.append(vertex)
    }
    adjacencyList.put(commandVertex, edgeList)
  }

  def findStronglyConnectedComponents(): ListBuffer[ListBuffer[Vertex]] = {
    var index: Int = 0
    var stack: ListBuffer[Vertex] = ListBuffer()
    var stronglyConnectedComponents: ListBuffer[ListBuffer[Vertex]] = ListBuffer()

    def isUndefined(vertex: Vertex): Boolean = {
      vertex.index == -1
    }

    def stronglyConnect(vertex: Vertex): Unit = {
      vertex.index = index
      vertex.lowLink = index
      index += 1
      stack.append(vertex)
      //println(stack)
      vertex.onStack = true

      println("contents: " + vertex.contents)
      println("First: " + vertex.index)
      println("lowlink: " + vertex.lowLink)

      if (adjacencyList.get(vertex).nonEmpty) {
        for (secondVertex <- adjacencyList.getOrElse(vertex, null)) {
          if (isUndefined(secondVertex)) {
            println(secondVertex)
            stronglyConnect(secondVertex)
            vertex.lowLink = Math.min(vertex.lowLink, secondVertex.lowLink)
          } else if (secondVertex.onStack) {
            vertex.lowLink = Math.min(vertex.lowLink, secondVertex.index)
          }
        }
      }

      if (vertex.lowLink == vertex.index) {
        val newComponent: ListBuffer[Vertex] = ListBuffer()
        var w: Vertex = null
        do {
          w = stack.last
          println(w)
          stack -= stack.last
          w.onStack = false
          newComponent.append(w)
        } while (!w.contents.equals(vertex.contents))
        stronglyConnectedComponents.append(newComponent)
      }
    }

    for (vertex <- adjacencyList.keys) {
      if (isUndefined(vertex)) {
        stronglyConnect(vertex)
      }
    }
    //println(stronglyConnectedComponents)
    stronglyConnectedComponents
  }

  def executeCommands(stateMachine: StateMachine): Unit = {
    val components: ListBuffer[ListBuffer[Vertex]] = findStronglyConnectedComponents()
    for (component <- components.reverse) {
      val sortedComponent: ListBuffer[Vertex] = component.sortBy(vertex => vertex.sequenceNum)
      for (vertex <- sortedComponent) {
        stateMachine.executeCommand(vertex.contents)
      }
    }
  }
}
