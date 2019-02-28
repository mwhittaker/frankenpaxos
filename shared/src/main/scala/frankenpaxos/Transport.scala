package frankenpaxos

import scala.concurrent.ExecutionContext

// # Overview
// A fundamental part of any distributed system is the ability for actors (aka
// nodes, processes) to send messages to other actors. There are many ways that
// actors can send messages to one another. Here are a few:
//
//   - Actors can send messages over the network using TCP.
//   - Actors can send messages over the network using UDP.
//   - If two actor are co-located on the same physical machine, they can send
//     messages through a shared file.
//
// Ideally, we could write a distributed system without having to worry about
// the lower-level details of exactly how messages are sent. The Transport
// class is designed to do exactly this. Actors in a distributed system can
// send and receive messages using a Transport object without having to know
// concretely how the Transport object is sending messages.
//
// We associate every actor with a unique address. The type of this address is
// dependent on the implementation of the Transport. For example, a Transport
// that sends messages over the network using TCP would probably use an (IP
// address, port number) pair as an address, while a different kind of
// Transport would probbaly use some other kind of address.
//
// Actors---specifically, subclasses of Actor---implicitly call the
// Transport#register method to register themselves with a transport. This
// allows them to receive messages from the Transport. Similarly, actors use
// the Transport#send method to send messages to other actors.
//
// # Timers
// Transport objects allow actors to send and receive messages. They also allow
// actors to set timers, using the Transport#timer method. By setting timers,
// an actor can run a lambda function after a set amount of time.
//
// # Threading
// All Transport implementations MUST be single-threaded. Actor `receive`
// methods and timer callbacks must be called serially on a single thread.
trait Transport[Self <: Transport[Self]] {
  // The type of address used to identify actors. Every Transport
  // implementation is free to use its own type of address.
  type Address <: frankenpaxos.Address
  def addressSerializer: Serializer[Self#Address]

  // The type of timer. As with Address, every Transport implementation is free
  // to use its own type of timer.
  type Timer <: frankenpaxos.Timer

  // Register an actor with a particular address. After an actor is registered
  // with an address, any messages sent to that address will be delivered to
  // the actor. Two actors cannot be registered to the same address. The Actor
  // class calls this method, so you should not have to.
  private[frankenpaxos] def register(
      address: Self#Address,
      actor: Actor[Self]
  ): Unit

  // Send a message from a source actor to a destination actor. You should not
  // call this method directly. Instead, use the send method inside Actor.
  private[frankenpaxos] def send(
      src: Self#Address,
      dst: Self#Address,
      bytes: Array[Byte]
  ): Unit

  // Create a named timer for a particular actor. You should not call this
  // method directly. Instead, use the timer method inside Actor.
  def timer(
      address: Self#Address,
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): Self#Timer

  // Every Transport implementation is a single-threaded event loop.
  // executionContext returns an ExecutionContext that you can use to schedule
  // events to run on this event loop.
  def executionContext(): ExecutionContext
}
