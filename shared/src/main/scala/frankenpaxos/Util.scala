package frankenpaxos

import collection.mutable

object Util {
  def histogram[T](xs: Iterable[T]): mutable.Map[T, Int] = {
    val counts = mutable.Map[T, Int]()
    for (x <- xs) {
      counts(x) = counts.getOrElse(x, 0) + 1
    }
    counts
  }

  // `popularItems(xs, n)` returns the elements in `xs` that appear `n` or more
  // times. For example,
  //
  //   popularItems(Seq('a', 'a', 'a', 'b', 'b', 'c'), 2) == Seq('a', 'b')
  def popularItems[T](xs: Iterable[T], n: Int): Set[T] = {
    { for ((x, count) <- histogram(xs); if count >= n) yield x }.to[Set]
  }
}
