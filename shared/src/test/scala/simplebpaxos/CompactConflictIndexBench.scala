package frankenpaxos.simplebpaxos

import frankenpaxos.statemachine
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._

object CompactConflictIndexBench extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  private var numLeaders = 5
  private var nextLeader = 0
  private var nextId = 0

  private def bytes(x: Int): Array[Byte] = {
    Array[Byte](x.toByte)
  }

  private def nextVertexId(): VertexId = {
    val leader = nextLeader
    val id = nextId

    if (leader + 1 == numLeaders) {
      nextLeader = 0
      nextId += 1
    } else {
      nextLeader += 1
    }

    VertexId(leader, id)
  }

  performance of "put" in {
    case class Params(numPut: Int)

    val params = for (numPut <- Gen.enumeration("numPut")(100000))
      yield Params(numPut)

    using(params) config (
      exec.independentSamples -> 5,
      exec.benchRuns -> 1,
    ) in { params =>
      val conflictIndex =
        new CompactConflictIndex(numLeaders, new statemachine.Noop())
      for (i <- 0 until params.numPut) {
        conflictIndex.put(nextVertexId(), bytes(42))
      }
    }
  }

  performance of "garbageCollect" in {
    case class Params(numGc: Int)

    val params = for (numGc <- Gen.enumeration("numGc")(100000))
      yield Params(numGc)

    using(params) config (
      exec.independentSamples -> 5,
      exec.benchRuns -> 1,
    ) in { params =>
      val conflictIndex =
        new CompactConflictIndex(numLeaders, new statemachine.Noop())
      for (i <- 0 until params.numGc) {
        conflictIndex.garbageCollect()
      }
    }
  }

  performance of "just getConflicts" in {
    case class Params(numGet: Int)

    val params = for (numGet <- Gen.enumeration("numGet")(100000))
      yield Params(numGet)

    using(params) config (
      exec.independentSamples -> 5,
      exec.benchRuns -> 10,
    ) in { params =>
      val conflictIndex =
        new CompactConflictIndex(numLeaders, new statemachine.Noop())
      for (i <- 0 until params.numGet) {
        conflictIndex.getConflicts(bytes(42))
      }
    }
  }

  performance of "put and garbageCollect" in {
    case class Params(
        numPut: Int,
        gcEvery: Int
    )

    val params =
      for {
        numPut <- Gen.enumeration("numPut")(100000)
        gcEvery <- Gen.enumeration("gcEvery")(1000000000, 10, 100, 1000, 10000)
      } yield Params(numPut, gcEvery)

    using(params) config (
      exec.independentSamples -> 5,
      exec.benchRuns -> 1,
    ) in { params =>
      val conflictIndex =
        new CompactConflictIndex(numLeaders, new statemachine.Noop())
      for (i <- 0 until params.numPut) {
        conflictIndex.put(nextVertexId(), bytes(42))
        if (i + 1 % params.gcEvery == 0) {
          conflictIndex.garbageCollect()
        }
      }
    }
  }

  performance of "put, garbageCollect, and getConflicts" in {
    case class Params(
        numPut: Int,
        gcEvery: Int,
        numGet: Int
    )

    val params =
      for {
        numPut <- Gen.enumeration("numPut")(100000)
        gcEvery <- Gen.enumeration("gcEvery")(1000)
        numGet <- Gen.enumeration("numGet")(100000)
      } yield Params(numPut, gcEvery, numGet)

    using(params) config (
      exec.independentSamples -> 2,
      exec.benchRuns -> 1,
    ) in { params =>
      val conflictIndex =
        new CompactConflictIndex(numLeaders, new statemachine.Noop())
      for (i <- 0 until params.numPut) {
        conflictIndex.put(nextVertexId(), bytes(42))
        if (i + 1 % params.gcEvery == 0) {
          conflictIndex.garbageCollect()
        }
      }
      for (_ <- 0 until params.numGet) {
        conflictIndex.getConflicts(bytes(42))
      }
    }
  }
}
