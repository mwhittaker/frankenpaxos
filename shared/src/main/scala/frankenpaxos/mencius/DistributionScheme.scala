package frankenpaxos.mencius

// Regular Mencius has clients, leaders, acceptors, and replicas. Decoupled
// Mencius adds batchers, proxy leaders, and proxy replicas.
//
// To avoid having to implement Mencius _and_ decoupled Mencius, we implement
// only decoupled Mencius. Then, to simulate Mencius, we omit the batchers, we
// run every leader with a co-located proxy leader, and we run every replica
// with a co-located proxy acceptor. When we do this co-location, leaders send
sealed trait DistributionScheme
case object Hash extends DistributionScheme
case object Colocated extends DistributionScheme
