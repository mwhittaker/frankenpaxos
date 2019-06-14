package frankenpaxos.monitoring

// The Counter trait mimics the Prometheus Java library's Counter class, and
// CounterImpl mimics the Counter.Child class [1].
//
// [1]: http://prometheus.github.io/client_java/io/prometheus/client/Counter.html

trait CounterBuilder[Self <: CounterBuilder[Self]]
    extends Builder[Self, Counter] {}

trait CounterImpl {
  def get(): Double
  def inc(): Unit
  def inc(amt: Double): Unit
}

trait Counter extends CounterImpl {
  type Impl <: CounterImpl
  def labels(labelValues: String*): Impl
  def get(): Double
  def inc(): Unit
  def inc(amt: Double): Unit
}
