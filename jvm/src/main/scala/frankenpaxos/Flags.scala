package frankenpaxos

object Flags {
  implicit val durationRead: scopt.Read[java.time.Duration] =
    scopt.Read.durationRead.map(x => java.time.Duration.ofNanos(x.toNanos))
}
