package zeno.examples

case class PaxosConfig[Transport <: zeno.Transport[Transport]](
    f: Int,
    proposerAddresses: List[Transport#Address],
    acceptorAddresses: List[Transport#Address]
) {
  def n: Int = (2 * f) + 1

  def valid(): Boolean = {
    return (proposerAddresses.size >= f + 1) && (acceptorAddresses.size == n)
  }
}
