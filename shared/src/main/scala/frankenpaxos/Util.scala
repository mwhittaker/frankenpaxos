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

  // Returns a duration sampled uniformly at random between min (inclusive) and
  // max (inclusive). For example,
  //
  //  randomDuration(
  //    java.time.Duration.ofSeconds(3),
  //    java.time.Duration.ofSeconds(5)
  //  )
  //
  // returns a random duration between 3 and 5 seconds.
  def randomDuration(
      min: java.time.Duration,
      max: java.time.Duration
  ): java.time.Duration = {
    val rand = java.util.concurrent.ThreadLocalRandom.current()
    val delta = max.minus(min)
    min.plus(java.time.Duration.ofNanos(rand.nextLong(0, delta.toNanos() + 1)))
  }
}
