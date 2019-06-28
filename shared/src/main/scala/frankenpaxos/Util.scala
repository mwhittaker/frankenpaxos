package frankenpaxos

import collection.mutable

object Util {
  // histogram(xs) returns a count of every element in xs.
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

  sealed trait MergeResult[L, R]
  case class Left[L, R](left: L) extends MergeResult[L, R]
  case class Both[L, R](left: L, right: R) extends MergeResult[L, R]
  case class Right[L, R](right: R) extends MergeResult[L, R]

  implicit class MapHelpers[K, L](left: Map[K, L]) {
    def merge[R, T](
        right: Map[K, R]
    )(f: (K, MergeResult[L, R]) => T): Map[K, T] = {
      val kvs = for (k <- left.keys ++ right.keys) yield {
        (left.get(k), right.get(k)) match {
          case (Some(l), None)    => k -> f(k, Left(l))
          case (Some(l), Some(r)) => k -> f(k, Both(l, r))
          case (None, Some(r))    => k -> f(k, Right(r))
          case (None, None)       => throw new IllegalStateException()
        }
      }
      kvs.toMap
    }
  }
}
