package frankenpaxos.fastpaxos

import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import org.scalatest._

class ConfigSpec extends FlatSpec {
  // Return a fabricated config with the given value of `f`.
  def makeConfig(f: Int): Config[FakeTransport] = {
    Config(
      f = f,
      leaderAddresses = for (i <- 1 to f + 1)
        yield FakeTransportAddress(s"Leader $i"),
      acceptorAddresses = for (i <- 1 to 2 * f + 1)
        yield FakeTransportAddress(s"Acceptor $i")
    )
  }

  "A Fast Paxos configuration" should "compute classic quorum sizes" in {
    assertResult(2)(makeConfig(1).classicQuorumSize)
    assertResult(3)(makeConfig(2).classicQuorumSize)
    assertResult(4)(makeConfig(3).classicQuorumSize)
  }

  it should "compute fast quorum sizes" in {
    assertResult(3)(makeConfig(1).fastQuorumSize)
    assertResult(4)(makeConfig(2).fastQuorumSize)
    assertResult(6)(makeConfig(3).fastQuorumSize)
    assertResult(7)(makeConfig(4).fastQuorumSize)
    assertResult(9)(makeConfig(5).fastQuorumSize)
  }
}
