package frankenpaxos

import com.github.tototoshi.csv.CSVWriter
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
  case class Timing(
      startTime: java.time.Instant,
      stopTime: java.time.Instant,
      durationNanos: Long
  )

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

  // Benchmark clients repeatedly issue requests to a service and record the
  // latency of each request. Aggregating these latency measurements, we can
  // compute statistics like average latency. Computing the rate of the
  // requests, we can compute statistics like average throughput.
  class Recorder(filename: String) {
    val writer = CSVWriter.open(new java.io.File(filename))
    writer.writeRow(Seq("start", "stop", "latency_nanos", "host", "port"))

    def record(
        start: java.time.Instant,
        stop: java.time.Instant,
        latencyNanos: Long,
        host: String,
        port: Int
    ): Unit = {
      writer.writeRow(
        Seq(start.toString(),
            stop.toString(),
            latencyNanos.toString(),
            host,
            port)
      )
    }
  }

  // A LabeledRecorder is like a recorder, but each command is annotated with a
  // label (e.g., "read" or "write").
  class LabeledRecorder(filename: String) {
    val writer = CSVWriter.open(new java.io.File(filename))
    writer.writeRow(
      Seq("start", "stop", "latency_nanos", "host", "port", "label")
    )

    def record(
        start: java.time.Instant,
        stop: java.time.Instant,
        latencyNanos: Long,
        host: String,
        port: Int,
        label: String
    ): Unit = {
      writer.writeRow(
        Seq(start.toString(),
            stop.toString(),
            latencyNanos.toString(),
            host,
            port,
            label)
      )
    }
  }
}
