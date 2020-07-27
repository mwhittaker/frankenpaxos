package frankenpaxos.craq

sealed trait DistributionScheme
case object Hash extends DistributionScheme
case object Colocated extends DistributionScheme
