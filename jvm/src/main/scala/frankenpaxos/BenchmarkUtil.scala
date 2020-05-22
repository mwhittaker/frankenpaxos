package frankenpaxos

import com.github.tototoshi.csv.CSVWriter
import scala.collection.mutable
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
  //
  // For systems with extremely high throughput, we can collapse measurements.
  // That is, clients can report multiple measurements as a single output. This
  // loses some fidelity but decreases data size and processing time.
  // Measurements are grouped by label.
  class LabeledRecorder(filename: String, groupSize: Int) {
    require(groupSize >= 1)

    case class Group(
        var count: Int = 0,
        var start: java.time.Instant = java.time.Instant.EPOCH,
        var stop: java.time.Instant = java.time.Instant.EPOCH,
        var latencyNanosSum: Long = 0
    )
    val groups = mutable.Map[String, Group]()

    // A measurement includes
    //
    //   - the start of the first request,
    //   - the stop time of the last request,
    //   - the number of measurements in the sample,
    //   - the average latency of the measurements in the sample, and
    //   - the label.
    val writer = CSVWriter.open(new java.io.File(filename))
    writer.writeRow(Seq("start", "stop", "count", "latency_nanos", "label"))

    private def resetGroup(group: Group): Unit = {
      group.count = 0
      group.start = java.time.Instant.EPOCH
      group.stop = java.time.Instant.EPOCH
      group.latencyNanosSum = 0
    }

    private def outputGroup(label: String, group: Group): Unit = {
      writer.writeRow(
        Seq(group.start.toString(),
            group.stop.toString(),
            group.count.toString(),
            (group.latencyNanosSum / group.count).toString(),
            label)
      )
    }

    def record(
        start: java.time.Instant,
        stop: java.time.Instant,
        latencyNanos: Long,
        label: String
    ): Unit = {
      // If we're not grouping measurements, then we don't need to jump through
      // hoops with labelled groups. We can output right away.
      if (groupSize == 1) {
        writer.writeRow(
          Seq(start.toString(),
              stop.toString(),
              "1",
              latencyNanos.toString(),
              label)
        )
        return
      }

      val group = groups.getOrElseUpdate(label, Group())
      group.count += 1
      if (group.count == 1) {
        group.start = start
      }
      group.stop = stop
      group.latencyNanosSum += latencyNanos

      if (group.count >= groupSize) {
        outputGroup(label, group)
        resetGroup(group)
      }
    }

    // Flush any pending groups.
    def flush(): Unit = {
      for ((label, group) <- groups) {
        if (group.count > 0) {
          outputGroup(label, group)
          resetGroup(group)
        }
      }
    }
  }
}
