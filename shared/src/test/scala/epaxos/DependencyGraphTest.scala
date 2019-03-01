package epaxos
import frankenpaxos.epaxos.{DependencyGraph, StateMachine}
import org.scalatest.FlatSpec

import scala.collection.mutable.ListBuffer

class DependencyGraphTest extends FlatSpec {

  "Dependency Graph execution" should "work correctly for a simple 6 node example" in {
    val commandOne: (String, Int) = ("0", 1)
    val commandTwo: (String, Int) = ("1", 2)
    val commandThree: (String, Int) = ("2", 3)
    val commandFour: (String, Int) = ("3", 4)
    val commandFive: (String, Int) = ("4", 5)
    val commandSix: (String, Int) = ("5", 6)

    val listBufferOne: ListBuffer[(String, Int)] = ListBuffer(commandTwo)
    val listBufferTwo: ListBuffer[(String, Int)] = ListBuffer(commandThree, commandFour, commandFive)
    val listBufferThree: ListBuffer[(String, Int)] = ListBuffer(commandOne, commandFive)
    val listBufferFour: ListBuffer[(String, Int)] = ListBuffer(commandSix)
    val listBufferFive: ListBuffer[(String, Int)] = ListBuffer(commandSix)
    val listBufferSix: ListBuffer[(String, Int)] = ListBuffer(commandFive)

    val graph: DependencyGraph = new DependencyGraph()
    graph.addNeighbors(commandOne, listBufferOne)
    graph.addNeighbors(commandTwo, listBufferTwo)
    graph.addNeighbors(commandThree, listBufferThree)
    graph.addNeighbors(commandFour, listBufferFour)
    graph.addNeighbors(commandFive, listBufferFive)
    graph.addNeighbors(commandSix, listBufferSix)

    val stateMachine: StateMachine = new StateMachine()
    graph.executeGraph(stateMachine)
    assert(ListBuffer(4, 5, 3, 0, 1, 2).toString().equals(stateMachine.getCurrentState()))
  }
}
