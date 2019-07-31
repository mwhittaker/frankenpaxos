package frankenpaxos.simplebpaxos

import scala.collection.mutable

// A QuorumWatermark involves a set of n integer-valued watermarks. A
// QuorumWatermarkVector involves a set of n vector-valued watermarks. For
// example, a QuorumWatermarkVector with 4 vectors of depth 3 looks like this:
//
//   [1, 2, 3]
//   [3, 2, 1]
//   [2, 4, 6]
//   [7, 5, 3]
//
// Given n watermarks, a QuorumWatermark returns the largest value w such that
// k or more watermarks are greater than or equal to w. Given n watermark
// vectors of depth d, a QuorumWatermarkVector returns the largest vector [w_0,
// ..., w_d] such that for every w_i, k or more watermark vectors are greater
// than or equal to w_i in position i. More intuitively, every column of the
// picture above acts as an independent QuorumWatermark.
class QuorumWatermarkVector(n: Int, depth: Int) {
  private val quorumWatermarks: Seq[QuorumWatermark] =
    for (_ <- 0 until depth) yield {
      new QuorumWatermark(numWatermarks = n)
    }

  override def toString(): String = quorumWatermarks.mkString("\n")

  def update(index: Int, watermark: Seq[Int]): Unit = {
    for ((watermark, quorumWatermark) <- watermark.zip(quorumWatermarks)) {
      quorumWatermark.update(index, watermark)
    }
  }

  def watermark(quorumSize: Int): Seq[Int] =
    quorumWatermarks.map(_.watermark(quorumSize))
}
