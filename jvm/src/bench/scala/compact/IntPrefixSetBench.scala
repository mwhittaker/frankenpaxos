// package frankenpaxos.compact
//
// import org.scalameter.api._
// import org.scalameter.picklers.Implicits._
// import org.scalameter.picklers.noPickler._
// import scala.collection.mutable
//
// object IntPrefixSetBenchmark extends Bench.ForkedTime {
//   override def aggregator: Aggregator[Double] = Aggregator.average
//
//   performance of "Set addition" in {
//     case class Params(numAdd: Int)
//
//     val params = for (numAdd <- Gen.enumeration("numAdd")(100000))
//       yield Params(numAdd)
//
//     using(params) config (
//       exec.independentSamples -> 1,
//       exec.benchRuns -> 1,
//     ) in { params =>
//       for (i <- 0 until params.numAdd) {
//         Set() ++ Set()
//       }
//     }
//   }
//
//   performance of "Set subtraction" in {
//     case class Params(numSub: Int)
//
//     val params = for (numSub <- Gen.enumeration("numSub")(100000))
//       yield Params(numSub)
//
//     using(params) config (
//       exec.independentSamples -> 1,
//       exec.benchRuns -> 1,
//     ) in { params =>
//       for (i <- 0 until params.numSub) {
//         Set() -- Set()
//       }
//     }
//   }
//
//   performance of "range" in {
//     case class Params(
//         low: Int,
//         high: Int,
//         n: Int
//     )
//
//     val params = for {
//       low <- Gen.enumeration("low")(0)
//       high <- Gen.enumeration("high")(100)
//       n <- Gen.enumeration("n")(100000)
//     } yield Params(low, high, n)
//
//     using(params) config (
//       exec.independentSamples -> 1,
//       exec.benchRuns -> 1,
//     ) in { params =>
//       for (_ <- 0 until params.n) {
//         Set() ++ (params.low until params.high)
//       }
//     }
//   }
//
//   performance of "for loop add" in {
//     case class Params(
//         low: Int,
//         high: Int,
//         n: Int
//     )
//
//     val params = for {
//       low <- Gen.enumeration("low")(0)
//       high <- Gen.enumeration("high")(100)
//       n <- Gen.enumeration("n")(100000)
//     } yield Params(low, high, n)
//
//     using(params) config (
//       exec.independentSamples -> 1,
//       exec.benchRuns -> 1,
//     ) in { params =>
//       for (_ <- 0 until params.n) {
//         val set = mutable.Set[Int]()
//         for (j <- params.low until params.high) {
//           set += j
//         }
//       }
//     }
//   }
//
//   performance of "add" in {
//     case class Params(numAdd: Int)
//
//     val params = for (numAdd <- Gen.enumeration("numAdd")(100000))
//       yield Params(numAdd)
//
//     using(params) config (
//       exec.independentSamples -> 1,
//       exec.benchRuns -> 1,
//     ) in { params =>
//       val prefixSet = IntPrefixSet()
//       for (i <- 0 until params.numAdd) {
//         prefixSet.add(i)
//       }
//     }
//   }
//
//   performance of "union" in {
//     case class Params(
//         lhs: Array[Byte],
//         rhs: Array[Byte],
//         numUnions: Int
//     )
//
//     val params =
//       for {
//         lhs <- Gen.enumeration("lhs")(
//           (0, Set[Int]()),
//           (10, Set[Int]()),
//           (1000, Set[Int]())
//         )
//         rhs <- Gen.enumeration("rhs")(
//           (0, Set[Int]()),
//           (10, Set[Int]()),
//           (1000, Set[Int]())
//         )
//         numUnions <- Gen.enumeration("numUnions")(100000)
//       } yield
//         Params(
//           IntPrefixSet(lhs._1, lhs._2).toProto.toByteArray,
//           IntPrefixSet(rhs._1, rhs._2).toProto.toByteArray,
//           numUnions
//         )
//
//     using(params) config (
//       exec.independentSamples -> 1,
//       exec.benchRuns -> 1,
//     ) in { params =>
//       val lhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.lhs))
//       val rhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.rhs))
//       for (_ <- 0 until params.numUnions) {
//         lhs.union(rhs)
//       }
//     }
//   }
//
//   performance of "diff" in {
//     case class Params(
//         lhs: Array[Byte],
//         rhs: Array[Byte],
//         numDiffs: Int
//     )
//
//     val params =
//       for {
//         lhs <- Gen.enumeration("lhs")(
//           (0, Set[Int]()),
//           (10, Set[Int]()),
//           (100, Set[Int]())
//         )
//         rhs <- Gen.enumeration("rhs")(
//           (0, Set[Int]()),
//           (20, Set[Int]()),
//           (150, Set[Int]())
//         )
//         numDiffs <- Gen.enumeration("numDiffs")(100000)
//       } yield
//         Params(
//           IntPrefixSet(lhs._1, lhs._2).toProto.toByteArray,
//           IntPrefixSet(rhs._1, rhs._2).toProto.toByteArray,
//           numDiffs
//         )
//
//     using(params) config (
//       exec.independentSamples -> 1,
//       exec.benchRuns -> 1,
//     ) in { params =>
//       val lhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.lhs))
//       val rhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.rhs))
//       for (_ <- 0 until params.numDiffs) {
//         lhs.diff(rhs)
//       }
//     }
//   }
//
//   performance of "subtractAll" in {
//     case class Params(
//         lhs: Array[Byte],
//         rhs: Array[Byte],
//         numSubtractAlls: Int
//     )
//
//     val params =
//       for {
//         lhs <- Gen.enumeration("lhs")(
//           (0, 0, 0),
//           (20, 0, 0),
//           (100, 0, 0)
//         )
//         rhs <- Gen.enumeration("rhs")(
//           (0, 0, 0),
//           (10, 0, 0)
//         )
//         numSubtractAlls <- Gen.enumeration("numSubtractAlls")(100000)
//       } yield
//         Params(
//           IntPrefixSet(lhs._1, (lhs._2 until lhs._3).toSet).toProto.toByteArray,
//           IntPrefixSet(rhs._1, (rhs._2 until rhs._3).toSet).toProto.toByteArray,
//           numSubtractAlls
//         )
//
//     using(params) config (
//       exec.independentSamples -> 1,
//       exec.benchRuns -> 1,
//     ) in { params =>
//       val lhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.lhs))
//       val rhs = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.rhs))
//       for (_ <- 0 until params.numSubtractAlls) {
//         lhs.subtractAll(rhs)
//       }
//     }
//   }
//
//   performance of "subtractOne" in {
//     case class Params(
//         set: Array[Byte],
//         x: Int,
//         numSubtractOnes: Int
//     )
//
//     val params =
//       for {
//         set <- Gen.enumeration("lhs")(
//           (0, 0, 0),
//           (20, 0, 0),
//           (100, 0, 0)
//         )
//         x <- Gen.enumeration("rhs")(0, 10, 50)
//         numSubtractOnes <- Gen.enumeration("numSubtractOnes")(100000)
//       } yield
//         Params(
//           IntPrefixSet(set._1, (set._2 until set._3).toSet).toProto.toByteArray,
//           x,
//           numSubtractOnes
//         )
//
//     using(params) config (
//       exec.independentSamples -> 1,
//       exec.benchRuns -> 1,
//     ) in { params =>
//       val set = IntPrefixSet.fromProto(IntPrefixSetProto.parseFrom(params.set))
//       for (_ <- 0 until params.numSubtractOnes) {
//         set.subtractOne(params.x)
//       }
//     }
//   }
// }
