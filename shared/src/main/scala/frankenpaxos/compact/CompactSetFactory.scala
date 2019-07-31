package frankenpaxos.compact

trait CompactSetFactory[CS <: CompactSet[CS] { type T = A }, A] {
  def empty: CS
  def fromSet(xs: Set[A]): CS
}
