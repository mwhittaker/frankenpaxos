package frankenpaxos

import collection.mutable

object Util {
  // `popularItems(xs, n)` returns the elements in `xs` that appear `n` or more
  // times. For example,
  //
  //   popularItems(Seq('a', 'a', 'a', 'b', 'b', 'c'), 2) == Seq('a', 'b')
  def popularItems[T](xs: Iterable[T], n: Int): Set[T] = {
    val counts = mutable.Map[T, Int]()
    for (x <- xs) {
      counts.get(x) match {
        case Some(count) => counts(x) = count + 1
        case None        => counts(x) = 1
      }
    }
    val ys = for ((x, count) <- counts; if count >= n) yield x
    ys.to[Set]
  }
}
