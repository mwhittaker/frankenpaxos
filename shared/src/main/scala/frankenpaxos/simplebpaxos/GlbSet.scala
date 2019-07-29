package frankenpaxos.simplebpaxos

import scala.collection.mutable

trait LatticeWithBottom[T] {
  def bottom: T
  def glb(lhs: T, rhs: T): T
  def lub(lhs: T, rhs: T): T
}

object LatticeWithBottom {
  implicit val intLatticeWithBottom = new LatticeWithBottom[Int] {
    override val bottom: Int = 0
    override def glb(lhs: Int, rhs: Int): Int = Math.min(lhs, rhs)
    override def lub(lhs: Int, rhs: Int): Int = Math.max(lhs, rhs)
  }

  def seqLatticeWithBottom[T](n: Int)(implicit lattice: LatticeWithBottom[T]) =
    new LatticeWithBottom[Seq[T]] {
      override val bottom: Seq[T] = for (_ <- 0 until n) yield lattice.bottom

      override def glb(lhs: Seq[T], rhs: Seq[T]): Seq[T] = {
        require(lhs.size == n)
        require(rhs.size == n)
        lhs.zip(rhs).map({ case (x, y) => lattice.glb(x, y) })
      }

      override def lub(lhs: Seq[T], rhs: Seq[T]): Seq[T] = {
        require(lhs.size == n)
        require(rhs.size == n)
        require(lhs.size == rhs.size)
        lhs.zip(rhs).map({ case (x, y) => lattice.lub(x, y) })
      }
    }
}

class GlbSet[T](n: Int)(implicit lattice: LatticeWithBottom[T]) {
  private val values: mutable.Buffer[T] = mutable.Buffer.fill(n)(lattice.bottom)
  private var valuesGlb: T = lattice.bottom

  def update(i: Int, value: T): Unit = {
    values(i) = lattice.lub(values(i), value)
    valuesGlb = lattice.glb(valuesGlb, values(i))
  }

  def glb() = valuesGlb
}
