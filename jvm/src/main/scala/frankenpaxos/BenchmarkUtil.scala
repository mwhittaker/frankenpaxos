package frankenpaxos

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Try

object BenchmarkUtil {
  // runUntil(f, deadline) runs f to produce a future. Once this future is
  // determined, runUntil runs f again to produce another future. And so on.
  // This process stops as soon as a future is determined after the deadline. A
  // couple things to note:
  //
  //   - Futures are not preempted at the deadline. If f returns a future that
  //     is never determined, then runUntil will never be determined. If f
  //     returns a future that is not determined until way after the deadline,
  //     then runUntil won't be determined until way after the deadline.
  //   - If f returns a future that results in a failure, it is ignored, and f
  //     is run again. If you don't want to ignore the result, then wrap f in a
  //     function that handles the result before you pass it to runUntil.
  def runUntil(
      f: () => Future[Unit],
      deadline: java.time.Instant
  )(implicit execution: ExecutionContext): Future[Unit] = {
    f().transformWith((_: Try[Unit]) => {
      if (java.time.Instant.now().isBefore(deadline)) {
        runUntil(f, deadline)
      } else {
        Future.successful(())
      }
    })
  }

  // See runUntil.
  def runFor(
      f: () => Future[Unit],
      duration: java.time.Duration
  )(implicit execution: ExecutionContext): Future[Unit] = {
    runUntil(f, java.time.Instant.now().plus(duration))
  }

  // timed(f) augments f to return timing information about how long it took f
  // to become determined.
  def timed[T](
      f: () => Future[T]
  )(implicit execution: ExecutionContext): Future[(T, Timing)] = {
    val startTime = java.time.Instant.now()
    val startTimeNanos = System.nanoTime()
    f().map((result) => {
      val stopTimeNanos = System.nanoTime()
      val stopTime = java.time.Instant.now()
      val timing = Timing(
        startTime = startTime,
        stopTime = stopTime,
        durationNanos = stopTimeNanos - startTimeNanos
      )
      (result, timing)
    })
  }

  case class Timing(
      startTime: java.time.Instant,
      stopTime: java.time.Instant,
      durationNanos: Long
  )
}
