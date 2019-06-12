package frankenpaxos

import scala.scalajs.js.annotation.JSExportAll

// A Timer is a function that gets invoked after a specified timeout.
// Typically, you construct a Timer using the `timer` method within actor. For
// example, the following actor prints "hello" every second.
//
//   class HelloActor[Transport <: frankenpaxos.Transport[Transport]](
//       val address: Transport#Address,
//       val transport: Transport,
//       val logger: Logger
//   ) extends Actor(address, transport, logger) {
//     ...
//
//     val helloTimer = timer(
//       "hello",
//       java.time.Duration.ofSeconds(1),
//       () => { println("hello") }
//     )
//   }
@JSExportAll
trait Timer {
  // Returns the name of the timer. Note that names do NOT uniquely identify
  // timers. Two timers can have the same name. Timer have names to make
  // debugging easier.
  def name(): String

  // Start the timer. If the timer is already started, calling start doesn't do
  // anything.
  def start(): Unit

  // Stop the timer. If the timer is already stopped, calling stop doesn't do
  // anything.
  def stop(): Unit

  // Stop and then start the timer.
  def reset(): Unit = {
    stop()
    start()
  }
}
