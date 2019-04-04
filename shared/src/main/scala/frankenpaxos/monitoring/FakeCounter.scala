package frankenpaxos.monitoring

class FakeCounter extends Counter {
  var count: Double = 0

  def get(): Double = count
  def inc(): Unit = count += 1
  def inc(amt: Double): Unit = count += amt
}
