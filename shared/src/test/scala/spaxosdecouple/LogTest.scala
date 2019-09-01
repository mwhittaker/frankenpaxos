package spaxosdecouple

import frankenpaxos.spaxosdecouple.Log
import org.scalatest._

class LogSpec extends FlatSpec {
  def matches[A](log: Log[A], has: Map[Int, A], no_has: Set[Int]): Boolean = {
    has.forall({ case (slot, value) => log.get(slot) == Some(value) }) &&
    no_has.forall(slot => log.get(slot) == None)
  }

  "An empty Log" should "work correctly" in {
    val log = new Log[Char]()
    assert(
      matches[Char](
        log,
        Map[Int, Char](),
        Set[Int](1, 2, 3, 4)
      ),
      log.toString()
    )
  }

  "A non-empty Log" should "work correctly" in {
    var log = new Log[Char]()
      .put(0, 'a')
      .put(1, 'b')
      .put(3, 'c')
      .putTail(5, 'd')
      .putTail(3, 'e')
      .putTail(7, 'f')

    assert(
      matches[Char](
        log,
        Map[Int, Char](0 -> 'a',
                       1 -> 'b',
                       3 -> 'e',
                       4 -> 'e',
                       5 -> 'e',
                       6 -> 'e',
                       7 -> 'f',
                       8 -> 'f',
                       9 -> 'f'),
        Set[Int](2)
      ),
      log.toString()
    )
  }
}
