package frankenpaxos.multipaxos

import frankenpaxos.Workload
import frankenpaxos.statemachine
import scala.util.Random

// In Evelyn Paxos, clients issue reads and writes differently. Writes are sent
// to batchers or leaders while reads are sent to acceptors and then replicas.
// A ReadWriteWorkload is like a Workload, except that it distinguishes between
// reads and writes.
sealed trait ReadWrite
case class Read(command: Array[Byte]) extends ReadWrite
case class Write(command: Array[Byte]) extends ReadWrite

trait ReadWriteWorkload {
  def get(): ReadWrite
}

// A UniformReadWriteWorkload is a key-value store workload. `readFraction` of
// all operations are reads. Every read and write picks one of `numKeys` keys
// uniformly at random. Write value sizes are governed by `writeSizeMean` and
// `writeSizeStd`.
class UniformReadWriteWorkload(
    numKeys: Int,
    readFraction: Float,
    writeSizeMean: Int,
    writeSizeStd: Int
) extends ReadWriteWorkload {
  override def toString(): String =
    s"UniformReadWriteWorkload(" +
      s"numKeys=$numKeys, readFraction=$readFraction, " +
      s"writeSizeMean=$writeSizeMean, writeSizeStd=$writeSizeStd)"

  override def get(): ReadWrite = {
    val key = Random.nextInt(numKeys).toString()
    if (Random.nextFloat() <= readFraction) {
      val command = statemachine
        .KeyValueStoreInput()
        .withGetRequest(statemachine.GetRequest(key = Seq(key)))
      Read(command.toByteArray)
    } else {
      val size =
        Math.max(
          0,
          (Random.nextGaussian() * writeSizeStd + writeSizeMean).round.toInt
        )
      val value = Random.nextString(size)
      val command = statemachine
        .KeyValueStoreInput()
        .withSetRequest(
          statemachine.SetRequest(
            keyValue = Seq(statemachine.SetKeyValuePair(key, value))
          )
        )
      Write(command.toByteArray)
    }
  }
}

// A UniformMultiKeyReadWriteWorkload is like a UniformReadWriteWorkload except
// with multiple keys. Every key is generated independently.
class UniformMultiKeyReadWriteWorkload(
    numKeys: Int,
    numOperations: Int,
    readFraction: Float,
    writeSizeMean: Int,
    writeSizeStd: Int
) extends ReadWriteWorkload {
  require(numKeys >= 1)
  require(numOperations >= 1)
  require(0 <= readFraction && readFraction <= 1)
  require(writeSizeMean >= 0)
  require(writeSizeStd >= 0)

  override def toString(): String =
    s"UniformReadWriteWorkload(" +
      s"numKeys=$numKeys, numOperations = $numOperations, " +
      s"readFraction=$readFraction, writeSizeMean=$writeSizeMean, " +
      s"writeSizeStd=$writeSizeStd)"

  override def get(): ReadWrite = {
    val keys = for (_ <- 0 until numOperations)
      yield "%4d".format(Random.nextInt(numKeys))
    if (Random.nextFloat() <= readFraction) {
      val command = statemachine
        .KeyValueStoreInput()
        .withGetRequest(statemachine.GetRequest(key = keys))
      Read(command.toByteArray)
    } else {
      val values = for (_ <- 0 until numOperations) yield {
        val size = Math.max(
          0,
          (Random.nextGaussian() * writeSizeStd + writeSizeMean).round.toInt
        )
        Random.nextString(size)
      }
      val command = statemachine
        .KeyValueStoreInput()
        .withSetRequest(
          statemachine.SetRequest(
            keyValue = keys
              .zip(values)
              .map({
                case (key, value) => statemachine.SetKeyValuePair(key, value)
              })
          )
        )
      Write(command.toByteArray)
    }
  }
}

// A WriteOnlyWorkload is a ReadWriteWorkload that wraps a Workload.
class WriteOnlyWorkload(workload: Workload) extends ReadWriteWorkload {
  override def toString(): String = s"WriteOnlyWorkload($workload)"
  override def get(): ReadWrite = Write(workload.get())
}

object ReadWriteWorkload {
  def fromProto(proto: ReadWriteWorkloadProto): ReadWriteWorkload = {
    import ReadWriteWorkloadProto.Value
    proto.value match {
      case Value.UniformReadWriteWorkload(w) =>
        new UniformReadWriteWorkload(
          numKeys = w.numKeys,
          readFraction = w.readFraction,
          writeSizeMean = w.writeSizeMean,
          writeSizeStd = w.writeSizeStd
        )
      case Value.UniformMultiKeyReadWriteWorkload(w) =>
        new UniformMultiKeyReadWriteWorkload(
          numKeys = w.numKeys,
          numOperations = w.numOperations,
          readFraction = w.readFraction,
          writeSizeMean = w.writeSizeMean,
          writeSizeStd = w.writeSizeStd
        )
      case Value.WriteOnlyStringWorkload(w) =>
        new WriteOnlyWorkload(Workload.fromProto(w.workload))
      case Value.WriteOnlyUniformSingleKeyWorkload(w) =>
        new WriteOnlyWorkload(Workload.fromProto(w.workload))
      case Value.WriteOnlyBernoulliSingleKeyWorkload(w) =>
        new WriteOnlyWorkload(Workload.fromProto(w.workload))
      case Value.Empty =>
        throw new IllegalArgumentException(
          "Empty ReadWriteWorkloadProto encountered."
        )
    }
  }

  def fromFile(filename: String): ReadWriteWorkload = {
    val source = scala.io.Source.fromFile(filename)
    try {
      fromProto(ReadWriteWorkloadProto.fromAscii(source.mkString))
    } finally {
      source.close()
    }
  }

  // Specifying a workload on the command line is a bit tricky since every
  // workload is parameterized by a number of variables. Instead of trying to
  // do something fancy with flags, we specify workloads using a proto.
  implicit val read: scopt.Read[ReadWriteWorkload] = scopt.Read.reads(fromFile)
}
