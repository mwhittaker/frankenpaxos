package frankenpaxos.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class BufferMapTest extends FlatSpec with Matchers {
  "A BufferMap" should "put within buffer correctly" in {
    val map = new BufferMap[String](10)
    map.get(0) shouldBe None
    map.put(0, "0")
    map.get(0) shouldBe Some("0")
  }

  it should "put beyond end of buffer correctly" in {
    val map = new BufferMap[String](10)
    map.get(100) shouldBe None
    map.put(100, "100")
    map.get(100) shouldBe Some("100")
  }

  it should "put beyond end of buffer correctly with 0 growsize" in {
    val map = new BufferMap[String](0)
    map.get(100) shouldBe None
    map.put(100, "100")
    map.get(100) shouldBe Some("100")
  }

  it should "put beyond end of buffer twice correctly" in {
    val map = new BufferMap[String](10)
    map.get(100) shouldBe None
    map.put(100, "100")
    map.get(100) shouldBe Some("100")
    map.get(1000) shouldBe None
    map.put(1000, "1000")
    map.get(1000) shouldBe Some("1000")
  }

  it should "garbage collect nothing correctly" in {
    val map = new BufferMap[String](10)
    map.get(0) shouldBe None
    map.put(0, "0")
    map.get(0) shouldBe Some("0")
    map.garbageCollect(0)
    map.get(0) shouldBe Some("0")
  }

  it should "garbage collect once correctly" in {
    val map = new BufferMap[String](10)
    map.put(0, "0")
    map.put(1, "1")
    map.put(2, "2")
    map.put(3, "3")
    map.garbageCollect(2)
    map.get(0) shouldBe None
    map.get(1) shouldBe None
    map.get(2) shouldBe Some("2")
    map.get(3) shouldBe Some("3")
  }

  it should "garbage collect low then high correctly" in {
    val map = new BufferMap[String](10)
    map.put(0, "0")
    map.put(1, "1")
    map.put(2, "2")
    map.put(3, "3")
    map.garbageCollect(1)
    map.get(0) shouldBe None
    map.get(1) shouldBe Some("1")
    map.get(2) shouldBe Some("2")
    map.get(3) shouldBe Some("3")
    map.garbageCollect(3)
    map.get(0) shouldBe None
    map.get(1) shouldBe None
    map.get(2) shouldBe None
    map.get(3) shouldBe Some("3")
  }

  it should "garbage collect high then low correctly" in {
    val map = new BufferMap[String](10)
    map.put(0, "0")
    map.put(1, "1")
    map.put(2, "2")
    map.put(3, "3")
    map.garbageCollect(3)
    map.get(0) shouldBe None
    map.get(1) shouldBe None
    map.get(2) shouldBe None
    map.get(3) shouldBe Some("3")
    map.garbageCollect(1)
    map.get(0) shouldBe None
    map.get(1) shouldBe None
    map.get(2) shouldBe None
    map.get(3) shouldBe Some("3")
  }

  it should "garbage collect beyond array correctly" in {
    val map = new BufferMap[String](10)
    map.garbageCollect(100)
    map.put(200, "200")
    map.get(200) shouldBe Some("200")
  }

  it should "put, gc, put, gc, put correctly" in {
    val map = new BufferMap[String](10)
    map.put(10, "10")
    map.put(20, "20")
    map.garbageCollect(15)
    map.put(30, "30")
    map.put(40, "40")
    map.garbageCollect(35)
    map.put(50, "50")
    map.put(60, "60")
    map.get(40) shouldBe Some("40")
    map.get(50) shouldBe Some("50")
    map.get(60) shouldBe Some("60")
  }
}
