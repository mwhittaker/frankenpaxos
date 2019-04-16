package frankenpaxos.thrifty

// In Paxos (and in pretty much every Paxos variant), there comes a time when a
// node has to send a message to a set of nodes and wait for a certain number
// of responses. For example,
//
//   - Paxos leaders send phase 1a/2a messages to acceptors and wait for a
//     quorum of phase 1b/2b responses.
//   - Fast Paxos leaders send phase 1a/2a messages to acceptors and wait for a
//     superquorum of phase 1a/2b responses.
//   - BPaxos nodes send commands to dependency service nodes and wait for a
//     quorum of responses.
//
// There is a common optimization, known as thriftiness, that these protocols
// often perform. Instead of a node sending a message to _all_ other nodes and
// waiting to hear back from n of them, the node only sends messages to n of
// them. If none of the n nodes have failed, this can reduce the number of
// messages that need to be sent. If some of the n nodes have failed, then
// thriftiness can end up decreasing performance.
//
// A ThriftySystem is a policy that determines which set of nodes to send
// messages to. For example NotThriftySystem doesn't perform any thriftiness,
// RandomThriftySystem chooses a random subset of nodes, and
// ClosestThriftySystem chooses the closest nodes. Note that the name "thrifty
// system" is a play on quorum systems [1].
//
// [1]: http://vukolic.com/QuorumsOrigin.pdf
trait ThriftySystem {
  // `delays` maps a set of addresses to their estimated delays. `min` is the
  // minimum number of responses we need to receive. `choose` returns a set of
  // addresses in `delays` that is of size at least `min`.
  def choose[Transport <: frankenpaxos.Transport[Transport]](
      delays: Map[Transport#Address, java.time.Duration],
      min: Int
  ): Set[Transport#Address]
}
