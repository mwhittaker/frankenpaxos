package frankenpaxos.statemachine

import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

class StateMachineTest extends FlatSpec with Matchers with PropertyChecks {
  private def bytes(x: Int): Array[Byte] = {
    Array[Byte](x.toByte)
  }

  // Noop //////////////////////////////////////////////////////////////////////
  "Noop conflict index" should "put correctly" in {
    val noop = new Noop()
    val conflictIndex = noop.conflictIndex[Int]()
    conflictIndex.put(0, bytes(0))
    conflictIndex.put(1, bytes(1))
    conflictIndex.put(2, bytes(2))
  }

  it should "remove correctly" in {
    val noop = new Noop()
    val conflictIndex = noop.conflictIndex[Int]()
    conflictIndex.put(0, bytes(0))
    val removed = conflictIndex.remove(0)
    removed shouldBe defined
    removed.get shouldBe bytes(0)
  }

  it should "getConflicts correctly" in {
    val noop = new Noop()
    val conflictIndex = noop.conflictIndex[Int]()
    conflictIndex.put(0, bytes(0))
    conflictIndex.put(1, bytes(1))
    conflictIndex.put(2, bytes(2))
    conflictIndex.getConflicts(bytes(0)) shouldBe Set()
    conflictIndex.getConflicts(bytes(1)) shouldBe Set()
    conflictIndex.getConflicts(bytes(2)) shouldBe Set()
  }

  it should "snapshot correctly" in {
    val noop = new Noop()
    noop.fromBytes(noop.toBytes())
  }

  // Register //////////////////////////////////////////////////////////////////
  "Register conflict index" should "put and get correctly" in {
    val register = new Register()
    val conflictIndex = register.conflictIndex[Int]()
    conflictIndex.put(0, bytes(0))
    conflictIndex.put(1, bytes(1))
    conflictIndex.put(2, bytes(2))
  }

  it should "remove correctly" in {
    val register = new Register()
    val conflictIndex = register.conflictIndex[Int]()
    conflictIndex.put(0, bytes(0))
    conflictIndex.put(1, bytes(1))
    val removed = conflictIndex.remove(0)
    removed shouldBe defined
    removed.get shouldBe bytes(0)
    conflictIndex.getConflicts(bytes(2)) shouldBe Set(1)
  }

  it should "getConflicts correctly" in {
    val register = new Register()
    val conflictIndex = register.conflictIndex[Int]()
    conflictIndex.put(0, bytes(0))
    conflictIndex.put(1, bytes(1))
    conflictIndex.put(2, bytes(2))
    conflictIndex.getConflicts(bytes(0)) shouldBe Set(0, 1, 2)
    conflictIndex.getConflicts(bytes(1)) shouldBe Set(0, 1, 2)
    conflictIndex.getConflicts(bytes(2)) shouldBe Set(0, 1, 2)
  }

  it should "snapshot correctly" in {
    val register = new Register()
    forAll(Gen.asciiPrintableStr) { (s: String) =>
      register.run(s.getBytes)
      register.fromBytes(register.toBytes())
      register.toString() shouldBe s
    }
  }

  // AppendLog /////////////////////////////////////////////////////////////////
  "AppendLog conflict index" should "put and get correctly" in {
    val log = new AppendLog()
    val conflictIndex = log.conflictIndex[Int]()
    conflictIndex.put(0, bytes(0))
    conflictIndex.put(1, bytes(1))
    conflictIndex.put(2, bytes(2))
  }

  it should "remove correctly" in {
    val log = new AppendLog()
    val conflictIndex = log.conflictIndex[Int]()
    conflictIndex.put(0, bytes(0))
    conflictIndex.put(1, bytes(1))
    val removed = conflictIndex.remove(0)
    removed shouldBe defined
    removed.get shouldBe bytes(0)
    conflictIndex.getConflicts(bytes(2)) shouldBe Set(1)
  }

  it should "getConflicts correctly" in {
    val log = new AppendLog()
    val conflictIndex = log.conflictIndex[Int]()
    conflictIndex.put(0, bytes(0))
    conflictIndex.put(1, bytes(1))
    conflictIndex.put(2, bytes(2))
    conflictIndex.getConflicts(bytes(0)) shouldBe Set(0, 1, 2)
    conflictIndex.getConflicts(bytes(1)) shouldBe Set(0, 1, 2)
    conflictIndex.getConflicts(bytes(2)) shouldBe Set(0, 1, 2)
  }

  it should "snapshot correctly" in {
    forAll(Gen.containerOf[Seq, String](Gen.asciiPrintableStr)) {
      (ss: Seq[String]) =>
        val log = new AppendLog()
        for (s <- ss) {
          log.run(s.getBytes)
        }
        log.fromBytes(log.toBytes())
        log.get shouldBe ss
    }
  }

  // KeyValueStore /////////////////////////////////////////////////////////////
  private def get(keys: String*): KeyValueStoreInput =
    KeyValueStoreInput().withGetRequest(GetRequest(keys))

  private def set(keys: String*): KeyValueStoreInput =
    KeyValueStoreInput().withSetRequest(
      SetRequest(keys.map(key => SetKeyValuePair(key = key, value = "")))
    )

  "Key-value store conflict index" should "put and get correctly" in {
    val kvs = new KeyValueStore()
    val conflictIndex = kvs.typedConflictIndex[Int]()
    conflictIndex.put(0, get("x", "y"))
    conflictIndex.put(1, get("x", "y"))
    conflictIndex.put(2, get("x", "y"))
  }

  it should "remove correctly" in {
    val kvs = new KeyValueStore()
    val conflictIndex = kvs.typedConflictIndex[Int]()
    conflictIndex.put(0, get("x", "y"))
    conflictIndex.put(1, get("x", "y"))
    val removed = conflictIndex.remove(0)
    removed shouldBe defined
    removed.get shouldBe get("x", "y")
    conflictIndex.getConflicts(set("x", "y")) shouldBe Set(1)
  }

  it should "getConflicts correctly" in {
    val kvs = new KeyValueStore()
    val conflictIndex = kvs.typedConflictIndex[Int]()
    conflictIndex.put(0, get("a", "b"))
    conflictIndex.put(0, get("x", "y"))
    conflictIndex.put(1, get("y", "z"))
    conflictIndex.put(2, set("y", "z"))
    conflictIndex.put(3, set("z"))

    conflictIndex.getConflicts(get("y")) shouldBe Set(2)
    conflictIndex.getConflicts(set("y")) shouldBe Set(0, 1, 2)
    conflictIndex.getConflicts(set("z")) shouldBe Set(1, 2, 3)
    conflictIndex.getConflicts(set("a")) shouldBe Set()
  }

  it should "snapshot correctly" in {
    val gen = for {
      n <- Gen.chooseNum(0, 100)
      keys <- Gen.containerOf[Seq, String](Gen.asciiPrintableStr)
      values <- Gen.containerOf[Seq, String](Gen.asciiPrintableStr)
    } yield keys.zip(values)

    forAll(gen) { (keyvals: Seq[(String, String)]) =>
      val kvs = new KeyValueStore()
      for ((k, v) <- keyvals) {
        kvs.typedRun(
          KeyValueStoreInput()
            .withSetRequest(SetRequest(Seq(SetKeyValuePair(k, v))))
        )
      }
      kvs.fromBytes(kvs.toBytes())
      kvs.get() shouldBe keyvals.toMap
    }
  }
}
