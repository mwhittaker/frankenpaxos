package frankenpaxos.clienttable

import scala.collection.mutable
import scala.scalajs.js.annotation._

// Replicated state machine protocols provide linearizability. This means that
// every client request appears to have been executed exactly once between when
// the client request was issued and when the client receives the corresponding
// response.
//
// Now, imagine a client issues a request x to a state machine replication
// protocol and after some time, it hasn't heard back a response. The client
// might re-send x to the protocol. State machine replication protocols must
// make sure not to _execute_ x more than once, even though x was _issued_ more
// than once.
//
// Protocols like Viewstamped Replication Revisited (VRR) [1] and Raft [2]
// ensure exactly once semantics using a client table. The main idea is as
// follows:
//
//   - On the client: Every client has a corresponding integer-valued client
//     id, initially 0. When a client sends a command to a state machine
//     replication, it annotates the command with its client id. When it hears
//     back the response, it increments its id.
//   - On the replicas: Every state machine replica maintains a client table
//     that records, for each client, the largest client id executed for the
//     client and the corresponding response. Replicas ignore requests with
//     smaller client ids, re-send replies for the largest client id, and
//     update the client table with new commands as they are executed.
//
// This simple client table works for VRR and Raft because they arrange
// commands into a totally ordered log and execute commands from clients in
// increasing id order. That is, for every client, a replica will execute
// command 0, then 1, then 2, and so on. Why? Well, if a client has command
// executed in slot j, then every slot less than j has been chosen. Thus, any
// subsequent request sent by the client will be placed in a log entry larger
// than j.
//
// For protocols like EPaxos or Generalized Paxos, things are more complicated.
// Consider the following scenario:
//
//   - A client issues command x with id 0.
//   - x is committed by the protocol.
//   - Replica A executes x and sends the result to the client.
//   - The client receives the result.
//   - The client issues unconflicting command y with id 1.
//   - y is committed by the protocol.
//   - Replica B learns that y is committed and executes it, then sends a reply
//     to the client.
//
// Note that B executed y (id 1) before x (id 0)! Unlike with VRR or Raft,
// replicas may execute commands out of command id order. Thus, B cannot simply
// record the largest client id executed for every client. If it did, it would
// never execute x because x has a smaller id than y.
//
// To avoid this scenario, we implement ClientTable, a more sophisticated
// client table. Every ClientTable instance caches the result of executing the
// largest client id command for each client, and it also records the set of
// all executed client ids. You should use a ClientTable like this:
//
//   val clientTable = ...
//   clientTable.executed(clientAddress, clientId) match {
//     case clientTable.NotExecuted =>
//       // You're free to execute the command.
//       output = executeTheCommand()
//       clientTable.execute(clientAddress, clientId, output)
//
//     case clientTable.Executed(Some(output)) =>
//       // The command has already been executed. Don't execute it again! This
//       // command is also the latest from the client, so we have its cached
//       // results.
//       returnOutputToClient(output)
//
//     case clientTable.Executed(None) =>
//       // The command has already been executed. Don't execute it again! This
//       // command is out of date, so we don't have its output cached. This is
//       // not a problem though because a client won't be interested in a
//       // result to a state command anyway.
//   }
//
// [1]: https://scholar.google.com/scholar?cluster=13000400770252658813
// [2]: https://scholar.google.com/scholar?cluster=15141061938475263862
@JSExportAll
object ClientTable {
  sealed trait ExecutedResult[+Output]
  object NotExecuted extends ExecutedResult[Nothing]
  case class Executed[Output](output: Option[Output])
      extends ExecutedResult[Output]
}

@JSExportAll
class ClientTable[ClientAddress, Output] {
  type ClientId = Int

  @JSExportAll
  case class ClientState(
      largestId: ClientId,
      largestOutput: Output,
      executedIds: PrefixSet
  )

  @JSExport
  protected val table = mutable.Map[ClientAddress, ClientState]()

  def executed(
      clientAddress: ClientAddress,
      clientId: ClientId
  ): ClientTable.ExecutedResult[Output] = {
    table.get(clientAddress) match {
      case None => ClientTable.NotExecuted

      case Some(state) =>
        if (clientId == state.largestId) {
          ClientTable.Executed(Some(state.largestOutput))
        } else if (state.executedIds.contains(clientId)) {
          ClientTable.Executed(None)
        } else {
          ClientTable.NotExecuted
        }
    }
  }

  def execute(
      clientAddress: ClientAddress,
      clientId: ClientId,
      output: Output
  ): Unit = {
    table.get(clientAddress) match {
      case None =>
        table(clientAddress) = ClientState(
          largestId = clientId,
          largestOutput = output,
          executedIds = new PrefixSet() + clientId
        )

      case Some(state) =>
        if (state.executedIds.contains(clientId)) {
          throw new IllegalArgumentException(
            s"$clientAddress has already executed $clientId."
          )
        }

        state.executedIds.add(clientId)
        if (clientId > state.largestId) {
          table(clientAddress) = state.copy(
            largestId = clientId,
            largestOutput = output
          )
        }
    }
  }
}
