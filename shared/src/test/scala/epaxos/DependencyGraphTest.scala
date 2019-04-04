package epaxos
import com.google.protobuf.ByteString
import frankenpaxos.epaxos.{Command, DependencyGraph, StateMachine}
import frankenpaxos.statemachine.KeyValueStore
import org.scalatest.FlatSpec

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DependencyGraphTest extends FlatSpec {

  "Dependency Graph execution" should "work correctly for a simple 6 node example" in {
    val commandOne: (Command, Int) = (Command(ByteString.copyFromUtf8("0"), 0, ByteString.copyFromUtf8("SET a 0")), 1)
    val commandTwo: (Command, Int) = (Command(ByteString.copyFromUtf8("0"), 0, ByteString.copyFromUtf8("SET a 1")), 2)
    val commandThree: (Command, Int) = (Command(ByteString.copyFromUtf8("0"), 0, ByteString.copyFromUtf8("SET a 2")), 3)
    val commandFour: (Command, Int) = (Command(ByteString.copyFromUtf8("0"), 0, ByteString.copyFromUtf8("SET a 3")), 4)
    val commandFive: (Command, Int) = (Command(ByteString.copyFromUtf8("0"), 0, ByteString.copyFromUtf8("SET a 4")), 5)
    val commandSix: (Command, Int) = (Command(ByteString.copyFromUtf8("0"), 0, ByteString.copyFromUtf8("SET a 5")), 6)

    val listBufferOne: ListBuffer[(Command, Int)] = ListBuffer(commandTwo)
    val listBufferTwo: ListBuffer[(Command, Int)] = ListBuffer(commandThree, commandFour, commandFive)
    val listBufferThree: ListBuffer[(Command, Int)] = ListBuffer(commandOne, commandFive)
    val listBufferFour: ListBuffer[(Command, Int)] = ListBuffer(commandSix)
    val listBufferFive: ListBuffer[(Command, Int)] = ListBuffer(commandSix)
    val listBufferSix: ListBuffer[(Command, Int)] = ListBuffer(commandFive)

    val graph: DependencyGraph = new DependencyGraph()
    graph.addCommands(commandOne, listBufferOne)
    graph.addCommands(commandTwo, listBufferTwo)
    graph.addCommands(commandThree, listBufferThree)
    graph.addCommands(commandFour, listBufferFour)
    graph.addCommands(commandFive, listBufferFive)
    graph.addCommands(commandSix, listBufferSix)

    val stateMachine: KeyValueStore = new KeyValueStore()
    graph.executeDependencyGraph(stateMachine, mutable.Set.empty)
    println(stateMachine.toString())
    assert(Map("a" -> 2).toString().equals(stateMachine.toString()))
  }

  "Dependency Graph" should "work on a single disconnected graph" in {
    val command: (Command, Int) = (Command(ByteString.copyFromUtf8("0"), 0, ByteString.copyFromUtf8("SET a 0")), 1)
    val graph: DependencyGraph = new DependencyGraph()
    graph.addCommands(command, ListBuffer.empty)
    val stateMachine: KeyValueStore = new KeyValueStore()
    graph.executeDependencyGraph(stateMachine, mutable.Set.empty)
    println(stateMachine.toString())
  }
}
