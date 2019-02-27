package frankenpaxos

import scala.scalajs.js.annotation._;

// TODO(mwhittaker): Clarify semantics of what happens when you start a timer
// that's already started or stop a timer that's already stopped.
@JSExportAll
trait Timer {
  def name(): String
  def start(): Unit
  def stop(): Unit
  def reset(): Unit = {
    stop();
    start();
  }
}
