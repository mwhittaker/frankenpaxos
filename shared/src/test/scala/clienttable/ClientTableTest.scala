package frankenpaxos.clienttable

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class ClientTableSpec extends FlatSpec with Matchers {
  "A client table" should "execute one command successfully" in {
    val clientTable = new ClientTable[String, String]()
    clientTable.executed("a", 0) shouldBe ClientTable.NotExecuted
    clientTable.execute("a", 0, "foo")
    clientTable.executed("a", 0) shouldBe ClientTable.Executed(Some("foo"))
  }

  it should "execute ascending commands successfully" in {
    val clientTable = new ClientTable[String, String]()
    clientTable.execute("a", 0, "foo")

    clientTable.executed("a", 1) shouldBe ClientTable.NotExecuted
    clientTable.execute("a", 1, "bar")
    clientTable.executed("a", 0) shouldBe ClientTable.Executed(None)
    clientTable.executed("a", 1) shouldBe ClientTable.Executed(Some("bar"))

    clientTable.executed("a", 2) shouldBe ClientTable.NotExecuted
    clientTable.execute("a", 2, "baz")
    clientTable.executed("a", 0) shouldBe ClientTable.Executed(None)
    clientTable.executed("a", 1) shouldBe ClientTable.Executed(None)
    clientTable.executed("a", 2) shouldBe ClientTable.Executed(Some("baz"))
  }

  it should "execute commands in random order successfully" in {
    val clientTable = new ClientTable[String, String]()
    clientTable.execute("a", 1, "a1")
    clientTable.execute("a", 0, "a0")
    clientTable.execute("b", 4, "b4")
    clientTable.execute("a", 2, "a2")
    clientTable.execute("b", 2, "b2")
    clientTable.execute("b", 0, "b0")
    clientTable.execute("b", 1, "b1")
    clientTable.execute("a", 4, "a4")

    clientTable.executed("a", 0) shouldBe ClientTable.Executed(None)
    clientTable.executed("a", 1) shouldBe ClientTable.Executed(None)
    clientTable.executed("a", 2) shouldBe ClientTable.Executed(None)
    clientTable.executed("a", 3) shouldBe ClientTable.NotExecuted
    clientTable.executed("a", 4) shouldBe ClientTable.Executed(Some("a4"))
    clientTable.executed("b", 0) shouldBe ClientTable.Executed(None)
    clientTable.executed("b", 1) shouldBe ClientTable.Executed(None)
    clientTable.executed("b", 2) shouldBe ClientTable.Executed(None)
    clientTable.executed("b", 3) shouldBe ClientTable.NotExecuted
    clientTable.executed("b", 4) shouldBe ClientTable.Executed(Some("b4"))
  }
}
