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
  "Noop" should "snapshot correctly" in {
    val noop = new Noop()
    noop.fromBytes(noop.toBytes())
  }

  // Register //////////////////////////////////////////////////////////////////
  "Register" should "snapshot correctly" in {
    val register = new Register()
    forAll(Gen.asciiPrintableStr) { (s: String) =>
      register.run(s.getBytes)
      register.fromBytes(register.toBytes())
      register.toString() shouldBe s
    }
  }

  // AppendLog /////////////////////////////////////////////////////////////////
  "AppendLog" should "snapshot correctly" in {
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
  "KeyValueStore" should "snapshot correctly" in {
    val gen = for {
      n <- Gen.chooseNum(0, 100)
      keys <- Gen.containerOf[Seq, String](Gen.asciiPrintableStr)
      values <- Gen.containerOf[Seq, String](Gen.asciiPrintableStr)
    } yield keys.zip(values)

    forAll(gen) { (keyvals: Seq[(String, String)]) =>
      val kvs = new KeyValueStore()
      for ((k, v) <- keyvals) {
        kvs.typedRun(
          KeyValueStoreInput(
            batch = Seq(KeyValueStoreRequest().withSetRequest(SetRequest(k, v)))
          )
        )
      }
      kvs.fromBytes(kvs.toBytes())
      kvs.get() shouldBe keyvals.toMap
    }
  }
}
