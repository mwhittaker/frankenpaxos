package frankenpaxos

import scala.collection.mutable
import scala.util.Random
import scala.util.parsing.combinator.RegexParsers

// Clients send commands to a state machine replication protocol. The type of
// commands that the clients send depends on the state machine. For example, a
// key-value store state machine expects very different commands from a
// register state machine.
//
// Even for a given state machine, the kinds of commands that a client can send
// can vary. For example, key-value store clients can run a very contended
// workload (i.e. lots of gets and sets to the same key) or a very uncontended
// workload.
//
// A Workload represents a particular workload for a particular state machine.
// Typically, a client will instantiate a Workload object and repeatedly invoke
// get to get a state machine command to issue to the state machine replication
// protocol.
trait Workload {
  def get(): Array[Byte]
}

// The Noop, AppendLog, and Register state machine take arbitrary strings
// (technically, Array[Byte]). StringWorkload produces strings with sizes drawn
// from a normal distribution.
class StringWorkload(sizeMean: Int, sizeStd: Int) extends Workload {
  override def toString(): String =
    s"StringWorkload(sizeMean=$sizeMean, sizeStd=$sizeStd)"

  override def get(): Array[Byte] = {
    val size =
      Math.max(0, (Random.nextGaussian() * sizeStd + sizeMean).round.toInt)
    Array.fill(size)(0)
  }
}

// A UniformSingleKeyWorkload consists of `numKeys` keys. We flip a coin to
// decide whether to get or set and then choose a key uniformly at random. If
// we decide to set, we create a random string with size drawn from a normal
// distribution.
class UniformSingleKeyWorkload(
    numKeys: Int,
    sizeMean: Int,
    sizeStd: Int
) extends Workload {
  override def toString(): String =
    s"UniformSingleKeyWorkload(" +
      s"numKeys=$numKeys, sizeMean=$sizeMean, sizeStd=$sizeStd)"

  override def get(): Array[Byte] = {
    val key = Random.nextInt(numKeys).toString()
    val command = if (Random.nextBoolean()) {
      statemachine.KeyValueStoreInput(
        batch = Seq(
          statemachine
            .KeyValueStoreRequest()
            .withGetRequest(statemachine.GetRequest(key = key))
        )
      )
    } else {
      val size =
        Math.max(0, (Random.nextGaussian() * sizeStd + sizeMean).round.toInt)
      val value = Random.nextString(size)
      statemachine.KeyValueStoreInput(
        batch = Seq(
          statemachine
            .KeyValueStoreRequest()
            .withSetRequest(statemachine.SetRequest(key, value))
        )
      )
    }
    command.toByteArray
  }
}

// A BernoulliSingleKeyWorkload sets key `x` with likelihood p and gets key `y`
// with likelihood 1 - p. Thus, p is the conflict rate.
class BernoulliSingleKeyWorkload(
    conflictRate: Float,
    sizeMean: Int,
    sizeStd: Int
) extends Workload {
  override def toString(): String =
    s"BernoulliSingleKeyWorkload(" +
      s"conflictRate=$conflictRate, sizeMean=$sizeMean, sizeStd=$sizeStd)"

  override def get(): Array[Byte] = {
    val command = if (Random.nextFloat() <= conflictRate) {
      var size = (Random.nextGaussian() * sizeStd + sizeMean).round.toInt
      size = Math.max(0, size)
      val value = Random.nextString(size)
      statemachine.KeyValueStoreInput(
        batch = Seq(
          statemachine
            .KeyValueStoreRequest()
            .withSetRequest(statemachine.SetRequest("x", value))
        )
      )
    } else {
      statemachine.KeyValueStoreInput(
        batch = Seq(
          statemachine
            .KeyValueStoreRequest()
            .withGetRequest(statemachine.GetRequest(key = "y"))
        )
      )
    }
    command.toByteArray
  }
}

// A BinomialSingleKeyWorkload is a batch of BernoulliSingleKeyWorkloads.
class BinomialSingleKeyWorkload(
    conflictRate: Float,
    sizeMean: Int,
    sizeStd: Int,
    n: Int
) extends Workload {
  override def toString(): String =
    s"BinomialSingleKeyWorkload(" +
      s"conflictRate=$conflictRate, sizeMean=$sizeMean, sizeStd=$sizeStd, n=$n)"

  override def get(): Array[Byte] = {
    val requests = mutable.Buffer[statemachine.KeyValueStoreRequest]()
    for (_ <- 0 until n) {
      val request = if (Random.nextFloat() <= conflictRate) {
        var size = (Random.nextGaussian() * sizeStd + sizeMean).round.toInt
        size = Math.max(0, size)
        val value = Random.nextString(size)
        statemachine
          .KeyValueStoreRequest()
          .withSetRequest(statemachine.SetRequest("x", value))

      } else {
        statemachine
          .KeyValueStoreRequest()
          .withGetRequest(statemachine.GetRequest(key = "y"))
      }
    }
    statemachine.KeyValueStoreInput(batch = requests.toSeq).toByteArray
  }
}

object Workload {
  def fromProto(proto: WorkloadProto): Workload = {
    import WorkloadProto.Value
    proto.value match {
      case Value.StringWorkload(w) =>
        new StringWorkload(sizeMean = w.sizeMean, sizeStd = w.sizeStd)
      case Value.UniformSingleKeyWorkload(w) =>
        new UniformSingleKeyWorkload(numKeys = w.numKeys,
                                     sizeMean = w.sizeMean,
                                     sizeStd = w.sizeStd)
      case Value.BernoulliSingleKeyWorkload(w) =>
        new BernoulliSingleKeyWorkload(conflictRate = w.conflictRate,
                                       sizeMean = w.sizeMean,
                                       sizeStd = w.sizeStd)
      case Value.BinomialSingleKeyWorkload(w) =>
        new BinomialSingleKeyWorkload(conflictRate = w.conflictRate,
                                      sizeMean = w.sizeMean,
                                      sizeStd = w.sizeStd,
                                      n = w.n)
      case Value.Empty =>
        throw new IllegalArgumentException("Empty WorkloadProto encountered.")
    }
  }

  def fromFile(filename: String): Workload = {
    val source = scala.io.Source.fromFile(filename)
    try {
      fromProto(WorkloadProto.fromAscii(source.mkString))
    } finally {
      source.close()
    }
  }

  // Specifying a workload on the command line is a bit tricky since every
  // workload is parameterized by a number of variables. Instead of trying to
  // do something fancy with flags, we specify workloads using a proto.
  implicit val read: scopt.Read[Workload] = scopt.Read.reads(fromFile)
}
