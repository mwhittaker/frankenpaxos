package frankenpaxos.spaxosdecouple

// Regular MultiPaxos has clients, leaders, acceptors, and replicas. Decoupled
// MultiPaxos adds batchers, proxy leaders, and proxy replicas.
//
// To avoid having to implement MultiPaxos _and_ decoupled MultiPaxos, we
// implement only decoupled MultiPaxos. Then, to simulate MultiPaxos, we omit
// the batchers, we run every leader with a co-located proxy leader, and we run
// every replica with a co-located proxy acceptor. When we do this co-location,
// leaders send
sealed trait DistributionScheme
case object Hash extends DistributionScheme
case object Colocated extends DistributionScheme
