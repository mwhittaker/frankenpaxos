package frankenpaxos.matchmakermultipaxos

// To evaluate Matchmaker MultiPaxos, we have to do things like fail nodes and
// force unprompted reconfigurations. To trigger these events, we have a driver
// node, a Driver, that sends messages to the other nodes telling them to
// perform certain actions (e.g., fail).
//
// The actions of the driver are determined by a DriverWorkload. Different workloads
// do different things. For example, one workload might force an acceptor
// reconfiguration. A different workload might force a matchmaker
// reconfiguration.
sealed trait DriverWorkload

// This is a dud workload in which the driver doesn't do anything.
object DoNothing extends DriverWorkload

// This is a testing workload in which the leader waits some delay and then
// repeatedly performs reconfigurations.
case class RepeatedLeaderReconfiguration(
    // The acceptors to reconfigure to.
    acceptors: Set[Int],
    // The delay to the first reconfiguration.
    delay: java.time.Duration,
    // The period between reconfigurations.
    period: java.time.Duration
) extends DriverWorkload

// A leader reconfiguration involves a warmup period of reconfigurations, then
// a sequence of reconfigurations, then an acceptor failure, and a final
// reconfiguration away from the failed acceptor.
case class LeaderReconfiguration(
    warmupDelay: java.time.Duration,
    warmupPeriod: java.time.Duration,
    warmupNum: Int,
    delay: java.time.Duration,
    period: java.time.Duration,
    num: Int,
    failureDelay: java.time.Duration,
    recoverDelay: java.time.Duration
) extends DriverWorkload

// This workload involves a number of matchmaker reconfigurations, failing a
// matchmaker, reconfiguring away from the failed matchmaker, and then
// performing a regular reconfiguration.
case class MatchmakerReconfiguration(
    delay: java.time.Duration,
    period: java.time.Duration,
    num: Int,
    failureDelay: java.time.Duration,
    recoverDelay: java.time.Duration,
    reconfigureDelay: java.time.Duration
) extends DriverWorkload

object DriverWorkload {
  def fromProto(proto: DriverWorkloadProto): DriverWorkload = {
    import DriverWorkloadProto.Value
    proto.value match {
      case Value.DoNothing(w) =>
        DoNothing

      case Value.RepeatedLeaderReconfiguration(w) =>
        RepeatedLeaderReconfiguration(
          acceptors = w.acceptor.toSet,
          delay = java.time.Duration.ofMillis(w.delayMs),
          period = java.time.Duration.ofMillis(w.periodMs)
        )

      case Value.LeaderReconfiguration(w) =>
        LeaderReconfiguration(
          warmupDelay = java.time.Duration.ofMillis(w.warmupDelayMs),
          warmupPeriod = java.time.Duration.ofMillis(w.warmupPeriodMs),
          warmupNum = w.warmupNum,
          delay = java.time.Duration.ofMillis(w.delayMs),
          period = java.time.Duration.ofMillis(w.periodMs),
          num = w.num,
          failureDelay = java.time.Duration.ofMillis(w.failureDelayMs),
          recoverDelay = java.time.Duration.ofMillis(w.recoverDelayMs)
        )

      case Value.MatchmakerReconfiguration(w) =>
        MatchmakerReconfiguration(
          delay = java.time.Duration.ofMillis(w.delayMs),
          period = java.time.Duration.ofMillis(w.periodMs),
          num = w.num,
          failureDelay = java.time.Duration.ofMillis(w.failureDelayMs),
          recoverDelay = java.time.Duration.ofMillis(w.recoverDelayMs),
          reconfigureDelay = java.time.Duration.ofMillis(w.reconfigureDelayMs)
        )

      case Value.Empty =>
        throw new IllegalArgumentException(
          "Empty DriverWorkloadProto encountered."
        )
    }
  }

  def fromFile(filename: String): DriverWorkload = {
    val source = scala.io.Source.fromFile(filename)
    try {
      fromProto(DriverWorkloadProto.fromAscii(source.mkString))
    } finally {
      source.close()
    }
  }

  // Specifying a driver workload on the command line is a bit tricky since
  // every driver workload is parameterized by a number of variables. Instead
  // of trying to do something fancy with flags, we specify driver workloads
  // using a proto.
  implicit val read: scopt.Read[DriverWorkload] = scopt.Read.reads(fromFile)
}
