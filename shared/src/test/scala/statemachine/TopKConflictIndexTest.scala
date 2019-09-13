package frankenpaxos.statemachine

import frankenpaxos.util.TopK
import frankenpaxos.util.TopOne
import frankenpaxos.util.VertexIdLike
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scala.collection.mutable

class TopKConflictIndexTest extends FlatSpec with Matchers {
  val like = new VertexIdLike[(Int, Int)] {
    override def leaderIndex(t: (Int, Int)): Int = t._1
    override def id(t: (Int, Int)): Int = t._2
    override def make(x: Int, y: Int): (Int, Int) = (x, y)
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

  private def putWithSnapshots(
      conflictIndex: ConflictIndex[(Int, Int), Array[Byte]]
  ): ConflictIndex[(Int, Int), Array[Byte]] = {
    conflictIndex.put((0, 0), bytes(0))
    conflictIndex.put((0, 1), bytes(0))
    conflictIndex.put((0, 2), bytes(0))
    conflictIndex.put((1, 0), bytes(1))
    conflictIndex.put((1, 1), bytes(1))
    conflictIndex.put((2, 0), bytes(2))
    conflictIndex.putSnapshot((3, 2))
    conflictIndex.putSnapshot((3, 0))
    conflictIndex.putSnapshot((5, 0))
    conflictIndex.putSnapshot((4, 1))
    conflictIndex.putSnapshot((4, 0))
    conflictIndex.putSnapshot((3, 1))
    conflictIndex
  }

  // Noop //////////////////////////////////////////////////////////////////////
  "Noop top-k conflict index" should "getConflicts correctly" in {
    val noop = new Noop()
    val conflictIndices = {
      for (k <- 1 until 5) yield {
        putPyramid(noop.topKConflictIndex[(Int, Int)](k, numLeaders = 3, like))
      }
    }.toSeq

    conflictIndices(0).getTopOneConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(0, 0, 0)
    conflictIndices(1).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int]()
      )
    conflictIndices(2).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int]()
      )
    conflictIndices(3).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int]()
      )
  }

  it should "getConflicts with snapshots correctly" in {
    val noop = new Noop()
    val conflictIndices = {
      for (k <- 1 until 5) yield {
        putWithSnapshots(
          noop.topKConflictIndex[(Int, Int)](k, numLeaders = 6, like)
        )
      }
    }.toSeq

    conflictIndices(0).getTopOneConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(0, 0, 0, 3, 2, 1)
    conflictIndices(1).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(2).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(3).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
  }

  // Register //////////////////////////////////////////////////////////////////
  "Register top-k conflict index" should "getConflicts correctly" in {
    val register = new Register()
    val conflictIndices = {
      for (k <- 1 until 5) yield {
        putPyramid(
          register.topKConflictIndex[(Int, Int)](k, numLeaders = 3, like)
        )
      }
    }.toSeq

    conflictIndices(0).getTopOneConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(3, 2, 1)
    conflictIndices(1).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(2).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(3).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
  }

  it should "getConflicts with snapshots correctly" in {
    val register = new Register()
    val conflictIndices = {
      for (k <- 1 until 5) yield {
        putWithSnapshots(
          register.topKConflictIndex[(Int, Int)](k, numLeaders = 6, like)
        )
      }
    }.toSeq

    conflictIndices(0).getTopOneConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(3, 2, 1, 3, 2, 1)
    conflictIndices(1).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(2).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(3).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
  }

  // AppendLog /////////////////////////////////////////////////////////////////
  "AppendLog top-k conflict index" should "getConflicts correctly" in {
    val appendLog = new AppendLog()
    val conflictIndices = {
      for (k <- 1 until 5) yield {
        putPyramid(
          appendLog.topKConflictIndex[(Int, Int)](k, numLeaders = 3, like)
        )
      }
    }.toSeq

    conflictIndices(0).getTopOneConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(3, 2, 1)
    conflictIndices(1).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(2).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(3).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
  }

  it should "getConflicts with snapshots correctly" in {
    val log = new AppendLog()
    val conflictIndices = {
      for (k <- 1 until 5) yield {
        putWithSnapshots(
          log.topKConflictIndex[(Int, Int)](k, numLeaders = 6, like)
        )
      }
    }.toSeq

    conflictIndices(0).getTopOneConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(3, 2, 1, 3, 2, 1)
    conflictIndices(1).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(2).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
    conflictIndices(3).getTopKConflicts(bytes(0)).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](0, 1, 2),
        mutable.SortedSet[Int](0, 1),
        mutable.SortedSet[Int](0)
      )
  }

  // KeyValueStore /////////////////////////////////////////////////////////////
  private def get(keys: String*): KeyValueStoreInput =
    KeyValueStoreInput().withGetRequest(GetRequest(keys))

  private def set(keys: String*): KeyValueStoreInput =
    KeyValueStoreInput().withSetRequest(
      SetRequest(keys.map(key => SetKeyValuePair(key = key, value = "")))
    )

  "Key-value store top-k conflict index" should
    "get complicated conflicts correctly" in {
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
    val index1 = put(
      kvs.typedTopKConflictIndex[(Int, Int)](k = 1, numLeaders = 3, like)
    )
    index1.getTopOneConflicts(get("x")).get() shouldBe
      mutable.Buffer(3, 4, 0)
    index1.getTopOneConflicts(get("y")).get() shouldBe
      mutable.Buffer(0, 4, 0)
    index1.getTopOneConflicts(get("z")).get() shouldBe
      mutable.Buffer(0, 0, 0)
    index1.getTopOneConflicts(set("x")).get() shouldBe
      mutable.Buffer(4, 4, 0)
    index1.getTopOneConflicts(set("y")).get() shouldBe
      mutable.Buffer(0, 4, 21)
    index1.getTopOneConflicts(set("z")).get() shouldBe
      mutable.Buffer(0, 0, 21)
    index1.getTopOneConflicts(get("x", "y", "z")).get() shouldBe
      mutable.Buffer(3, 4, 0)
    index1.getTopOneConflicts(set("x", "y", "z")).get() shouldBe
      mutable.Buffer(4, 4, 21)

    // k = 2
    val index2 = put(
      kvs.typedTopKConflictIndex[(Int, Int)](k = 2, numLeaders = 3, like)
    )
    index2.getTopKConflicts(get("x")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](2),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int]()
      )
    index2.getTopKConflicts(get("y")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int]()
      )
    index2.getTopKConflicts(get("z")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int]()
      )
    index2.getTopKConflicts(set("x")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](2, 3),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int]()
      )
    index2.getTopKConflicts(set("y")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int](10, 20)
      )
    index2.getTopKConflicts(set("z")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](10, 20)
      )
    index2.getTopKConflicts(get("x", "y", "z")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](2),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int]()
      )
    index2.getTopKConflicts(set("x", "y", "z")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](2, 3),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int](10, 20)
      )
  }

  it should "get complicated conflicts with snapshots correctly" in {
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
      conflictIndex.putSnapshot((3, 0))
      conflictIndex.putSnapshot((3, 10))
      conflictIndex.putSnapshot((3, 20))
      conflictIndex

      // snapshots
      // (3, 0), (3, 10), (3, 20)
      //
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
    val index1 = put(
      kvs.typedTopKConflictIndex[(Int, Int)](k = 1, numLeaders = 4, like)
    )
    index1.getTopOneConflicts(get("x")).get() shouldBe
      mutable.Buffer(3, 4, 0, 21)
    index1.getTopOneConflicts(get("y")).get() shouldBe
      mutable.Buffer(0, 4, 0, 21)
    index1.getTopOneConflicts(get("z")).get() shouldBe
      mutable.Buffer(0, 0, 0, 21)
    index1.getTopOneConflicts(set("x")).get() shouldBe
      mutable.Buffer(4, 4, 0, 21)
    index1.getTopOneConflicts(set("y")).get() shouldBe
      mutable.Buffer(0, 4, 21, 21)
    index1.getTopOneConflicts(set("z")).get() shouldBe
      mutable.Buffer(0, 0, 21, 21)
    index1.getTopOneConflicts(get("x", "y", "z")).get() shouldBe
      mutable.Buffer(3, 4, 0, 21)
    index1.getTopOneConflicts(set("x", "y", "z")).get() shouldBe
      mutable.Buffer(4, 4, 21, 21)

    // k = 2
    val index2 = put(
      kvs.typedTopKConflictIndex[(Int, Int)](k = 2, numLeaders = 4, like)
    )
    index2.getTopKConflicts(get("x")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](2),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](10, 20)
      )
    index2.getTopKConflicts(get("y")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](10, 20)
      )
    index2.getTopKConflicts(get("z")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](10, 20)
      )
    index2.getTopKConflicts(set("x")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](2, 3),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](10, 20)
      )
    index2.getTopKConflicts(set("y")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int](10, 20),
        mutable.SortedSet[Int](10, 20)
      )
    index2.getTopKConflicts(set("z")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](10, 20),
        mutable.SortedSet[Int](10, 20)
      )
    index2.getTopKConflicts(get("x", "y", "z")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](2),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](10, 20)
      )
    index2.getTopKConflicts(set("x", "y", "z")).get() shouldBe
      mutable.Buffer(
        mutable.SortedSet[Int](2, 3),
        mutable.SortedSet[Int](1, 3),
        mutable.SortedSet[Int](10, 20),
        mutable.SortedSet[Int](10, 20)
      )
  }
}
