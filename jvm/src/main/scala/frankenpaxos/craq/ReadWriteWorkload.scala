package frankenpaxos.craq

// import frankenpaxos.statemachine
import scala.util.Random

// In Craq, clients issue reads and writes differently. Writes are sent to the
// head node, while reads are sent to any of the chain nodes. A
// ReadWriteWorkload is like a Workload, except that it distinguishes between
// reads and writes. Note that Craq, unlike protocols like MultiPaxos, can not
// be run with an arbitrary state machine. Craq is hardcoded with a string to
// string key-value store.
trait ReadWriteWorkload {
  sealed trait ReadWrite
  case class Read(key: String) extends ReadWrite
  case class Write(key: String, value: String) extends ReadWrite

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
    if (Random.nextFloat() <= readFraction) {
      Read(key = Random.nextInt(numKeys).toString())
    } else {
      val size = Math.max(
        0,
        (Random.nextGaussian() * writeSizeStd + writeSizeMean).round.toInt
      )
      Write(key = Random.nextInt(numKeys).toString(),
            value = Random.nextString(size))
    }
  }
}

// A PointSkewedReadWriteWorkload is a key-value store workload with `numKeys`
// keys. `readFraction` of all operations are reads, and `pointFraction` of all
// read and write operations are to a single key. `1 - pointFraction` of all
// other operations are to some other key chosen uniformly at random. This
// workload lets us test skewed workloads in an intuitive way. For example, if
// we set pointFraction to 1, then all writes are to the same key, and if we
// sent pointFraction to 0, then all operations are to a random key. This is a
// little more intuitive than varying zipf coefficients.
class PointSkewedReadWriteWorkload(
    numKeys: Int,
    readFraction: Float,
    pointFraction: Float,
    writeSizeMean: Int,
    writeSizeStd: Int
) extends ReadWriteWorkload {
  override def toString(): String =
    s"PointSkewedReadWriteWorkload(" +
      s"numKeys=$numKeys, readFraction=$readFraction, " +
      s"pointFraction=$pointFraction, writeSizeMean=$writeSizeMean, " +
      s"writeSizeStd=$writeSizeStd)"

  override def get(): ReadWrite = {
    val key = if (Random.nextFloat() <= pointFraction) {
      "0"
    } else {
      1 + Random.nextInt(numKeys - 1).toString()
    }

    if (Random.nextFloat() <= readFraction) {
      Read(key = key)
    } else {
      val size = Math.max(
        0,
        (Random.nextGaussian() * writeSizeStd + writeSizeMean).round.toInt
      )
      Write(key = key, value = Random.nextString(size))
    }
  }
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

      case Value.PointSkewedReadWriteWorkload(w) =>
        new PointSkewedReadWriteWorkload(
          numKeys = w.numKeys,
          readFraction = w.readFraction,
          pointFraction = w.pointFraction,
          writeSizeMean = w.writeSizeMean,
          writeSizeStd = w.writeSizeStd
        )

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
