package zeno

// class FakeTransportAddress extends Address {
//   def start(): Unit = {}
//   def stop(): Unit = {}
//   def reset(): Unit = {}
// }
//
// class FakeTransportTimer extends Timer {
//   def start(): Unit = {}
//   def stop(): Unit = {}
//   def reset(): Unit = {}
// }
//
// class FakeTransport extends Transport[FakeTransport] {
//   type A = FakeTransportAddress
//   type T = FakeTransportTimer
//
//   def timer(duration: java.time.Duration): T = {
//     ???
//   }
//
//   def register(address: A, actor: Actor[FakeTransport]): Unit = {}
//
//   def send(dst: A, msg: String): Unit = {
//     println(s"Sending $msg to $dst")
//   }
// }
