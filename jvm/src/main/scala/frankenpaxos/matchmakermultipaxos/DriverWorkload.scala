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

// A double leader reconfiguration involves two acceptor reconfigurations (both
// initiated by the leader) and an acceptor failure. The throughput should look
// something like this:
//
//                  a   b     c
//              |   |   |     |
//              |   |   |     |
//   throughput | --|-- |     |  -------
//              |   |  \|_____|_/
//              |   |   |     |
//              +------------------------
//                         time
// where
//
//   (a) we reconfigure,
//   (b) we fail an acceptor, and
//   (c) we reconfigure.
case class DoubleLeaderReconfiguration(
    // The delay to the first reconfiguration, and the set of acceptor indices
    // to reconfigure to.
    firstReconfigurationDelay: java.time.Duration,
    firstReconfiguration: Set[Int],
    // The delay to fail the acceptor, and the acceptor index to fail.
    acceptorFailureDelay: java.time.Duration,
    acceptorFailure: Int,
    // The delay to the second reconfiguration, and the set of acceptor indices
    // to reconfigure to.
    secondReconfigurationDelay: java.time.Duration,
    secondReconfiguration: Set[Int]
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

      case Value.DoubleLeaderReconfiguration(w) =>
        DoubleLeaderReconfiguration(
          firstReconfigurationDelay =
            java.time.Duration.ofMillis(w.firstReconfigurationDelayMs),
          firstReconfiguration = w.firstReconfiguration.toSet,
          acceptorFailureDelay =
            java.time.Duration.ofMillis(w.acceptorFailureDelayMs),
          acceptorFailure = w.acceptorFailure,
          secondReconfigurationDelay =
            java.time.Duration.ofMillis(w.secondReconfigurationDelayMs),
          secondReconfiguration = w.secondReconfiguration.toSet
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
