package frankenpaxos.statemachine

import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class TopKConflictIndexTest extends FlatSpec with Matchers {
  val intTuple = new VertexIdLike[(Int, Int)] {
    def leaderIndex(t: (Int, Int)): Int = {
      val (x, _) = t
      x
    }

    def id(t: (Int, Int)): Int = {
      val (_, y) = t
      y
    }
  }

  private def bytes(x: Int): Array[Byte] = {
    Array[Byte](x.toByte)
  }

  private def putPyramid(
      conflictIndex: ConflictIndex[(Int, Int), Array[Byte]]
  ): ConflictIndex[(Int, Int), Array[Byte]] = {
    conflictIndex.put((0, 0), bytes(0))
    conflictIndex.put((0, 1), bytes(0))
    conflictIndex.put((0, 2), bytes(0))
    conflictIndex.put((1, 0), bytes(1))
    conflictIndex.put((1, 1), bytes(1))
    conflictIndex.put((2, 0), bytes(2))
    conflictIndex
  }

  // Noop //////////////////////////////////////////////////////////////////////
  "Noop top-k conflict index" should "getConflicts correctly" in {
    val noop = new Noop()
    val conflictIndices = {
      for (k <- 1 until 5) yield {
        putPyramid(noop.topKConflictIndex[(Int, Int)](k, intTuple))
      }
    }.toSeq

    conflictIndices(0).getConflicts(bytes(0)) shouldBe Set()
    conflictIndices(1).getConflicts(bytes(0)) shouldBe Set()
    conflictIndices(2).getConflicts(bytes(0)) shouldBe Set()
    conflictIndices(3).getConflicts(bytes(0)) shouldBe Set()
  }

  // Register //////////////////////////////////////////////////////////////////
  "Register top-k conflict index" should "getConflicts correctly" in {
    val register = new Register()
    val conflictIndices = {
      for (k <- 1 until 5) yield {
        putPyramid(register.topKConflictIndex[(Int, Int)](k, intTuple))
      }
    }.toSeq

    conflictIndices(0).getConflicts(bytes(0)) shouldBe
      Set((0, 2), (1, 1), (2, 0))
    conflictIndices(1).getConflicts(bytes(0)) shouldBe
      Set((0, 1), (0, 2), (1, 0), (1, 1), (2, 0))
    conflictIndices(2).getConflicts(bytes(0)) shouldBe
      Set((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (2, 0))
    conflictIndices(3).getConflicts(bytes(0)) shouldBe
      Set((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (2, 0))
  }

  // AppendLog /////////////////////////////////////////////////////////////////
  "AppendLog top-k conflict index" should "getConflicts correctly" in {
    val appendLog = new AppendLog()
    val conflictIndices = {
      for (k <- 1 until 5) yield {
        putPyramid(appendLog.topKConflictIndex[(Int, Int)](k, intTuple))
      }
    }.toSeq

    conflictIndices(0).getConflicts(bytes(0)) shouldBe
      Set((0, 2), (1, 1), (2, 0))
    conflictIndices(1).getConflicts(bytes(0)) shouldBe
      Set((0, 1), (0, 2), (1, 0), (1, 1), (2, 0))
    conflictIndices(2).getConflicts(bytes(0)) shouldBe
      Set((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (2, 0))
    conflictIndices(3).getConflicts(bytes(0)) shouldBe
      Set((0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (2, 0))
  }

  // KeyValueStore /////////////////////////////////////////////////////////////
  private def get(keys: String*): KeyValueStoreInput =
    KeyValueStoreInput().withGetRequest(GetRequest(keys))

  private def set(keys: String*): KeyValueStoreInput =
    KeyValueStoreInput().withSetRequest(
      SetRequest(keys.map(key => SetKeyValuePair(key = key, value = "")))
    )

  "Key-value store top-k conflict index" should "get no conflicts correctly" in {
    for (k <- 1 until 5) {
      val kvs = new KeyValueStore()
      val conflictIndex = kvs.typedTopKConflictIndex[(Int, Int)](k, intTuple)
      conflictIndex.getConflicts(get("a")) shouldBe Set()
    }
  }

  it should "get complicated conflicts correctly" in {
    def put(
        conflictIndex: ConflictIndex[(Int, Int), KeyValueStoreInput]
    ): ConflictIndex[(Int, Int), KeyValueStoreInput] = {
      conflictIndex.put((0, 0), get("x"))
      conflictIndex.put((1, 3), set("x", "y"))
      conflictIndex.put((2, 20), get("y", "z"))
      conflictIndex.put((2, 10), get("y", "z"))
      conflictIndex.put((0, 1), get("x"))
      conflictIndex.put((0, 3), get("x"))
      conflictIndex.put((0, 2), set("x"))
      conflictIndex.put((1, 1), set("x", "y"))
      conflictIndex.put((2, 20), get("y", "z"))
      conflictIndex

      // gets
      // x: (0, 0), (0, 1), (0, 3)
      // y: (2, 10), (2, 20)
      // z: (2, 10), (2, 20)
      //
      // sets
      // x: (0, 2), (1, 1), (1, 3)
      // y: (1, 1), (1, 3)
      // z:
    }

    val kvs = new KeyValueStore()

    // k = 1
    val index1 = put(kvs.typedTopKConflictIndex[(Int, Int)](k = 1, intTuple))
    index1.getConflicts(get("x")) shouldBe Set((0, 2), (1, 3))
    index1.getConflicts(get("y")) shouldBe Set((1, 3))
    index1.getConflicts(get("z")) shouldBe Set()
    index1.getConflicts(set("x")) shouldBe Set((0, 3), (1, 3))
    index1.getConflicts(set("y")) shouldBe Set((1, 3), (2, 20))
    index1.getConflicts(set("z")) shouldBe Set((2, 20))
    index1.getConflicts(get("x", "y", "z")) shouldBe Set((0, 2), (1, 3))
    index1.getConflicts(set("x", "y", "z")) shouldBe Set((0, 3),
                                                         (1, 3),
                                                         (2, 20))
    // k = 2
    val index2 = put(kvs.typedTopKConflictIndex[(Int, Int)](k = 2, intTuple))
    index2.getConflicts(get("x")) shouldBe Set((0, 2), (1, 1), (1, 3))
    index2.getConflicts(get("y")) shouldBe Set((1, 1), (1, 3))
    index2.getConflicts(get("z")) shouldBe Set()
    index2.getConflicts(set("x")) shouldBe Set((0, 2), (0, 3), (1, 1), (1, 3))
    index2.getConflicts(set("y")) shouldBe Set((1, 1), (1, 3), (2, 10), (2, 20))
    index2.getConflicts(set("z")) shouldBe Set((2, 10), (2, 20))
    index2.getConflicts(get("x", "y", "z")) shouldBe Set((0, 2), (1, 1), (1, 3))
    index2.getConflicts(set("x", "y", "z")) shouldBe
      Set((0, 2), (0, 3), (1, 1), (1, 3), (2, 10), (2, 20))
  }
}
