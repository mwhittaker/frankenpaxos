package frankenpaxos.multipaxos

// Regular MultiPaxos has clients, leaders, acceptors, and replicas. Decoupled
// MultiPaxos adds batchers, proxy leaders, and proxy replicas.
//
// To avoid having to implement MultiPaxos _and_ decoupled MultiPaxos, we
// implement only decoupled MultiPaxos. Then, to simulate MultiPaxos, we run
// every leader with a co-located batcher and proxy leader, and we run every
// replica with a co-located proxy replica. When we do this co-location,
// clients send to the batcher co-located with the leader, leaders send to the
// co-located proxy leader, and replicas send to the co-located proxy replica.
sealed trait DistributionScheme
case object Hash extends DistributionScheme
case object Colocated extends DistributionScheme
