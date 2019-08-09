package frankenpaxos.simplebpaxos

import frankenpaxos.compact.IntPrefixSet
import frankenpaxos.statemachine.GetRequest
import frankenpaxos.statemachine.KeyValueStore
import frankenpaxos.statemachine.KeyValueStoreInput
import frankenpaxos.statemachine.SetKeyValuePair
import frankenpaxos.statemachine.SetRequest
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class CompactConflictIndexTest extends FlatSpec with Matchers {
  // DO_NOT_SUBMIT(mwhittaker): Fix tests.

  // private def get(keys: String*): KeyValueStoreInput =
  //   KeyValueStoreInput().withGetRequest(GetRequest(keys))
  //
  // private def set(keys: String*): KeyValueStoreInput =
  //   KeyValueStoreInput().withSetRequest(
  //     SetRequest(keys.map(key => SetKeyValuePair(key = key, value = "")))
  //   )
  //
  // "A CompactConflictIndex" should "put and getConflicts correctly" in {
  //   val kvs = new KeyValueStore()
  //   val conflictIndex = new CompactConflictIndex(kvs.typedConflictIndex[Int]())(
  //     IntPrefixSet.factory
  //   )
  //   conflictIndex.put(0, set("a"))
  //   conflictIndex.put(1, set("b"))
  //   conflictIndex.put(2, set("a"))
  //   conflictIndex.put(3, set("b"))
  //   conflictIndex.getConflicts(set("a")) shouldBe IntPrefixSet(Set(0, 2))
  //   conflictIndex.getConflicts(set("b")) shouldBe IntPrefixSet(Set(1, 3))
  // }
  //
  // "A CompactConflictIndex" should "garbageCollect correctly" in {
  //   val kvs = new KeyValueStore()
  //   val conflictIndex = new CompactConflictIndex(kvs.typedConflictIndex[Int]())(
  //     IntPrefixSet.factory
  //   )
  //   conflictIndex.put(0, set("a"))
  //   conflictIndex.put(1, set("a"))
  //   conflictIndex.put(2, set("a"))
  //   conflictIndex.put(3, set("a"))
  //   conflictIndex.garbageCollect(IntPrefixSet(Set(0, 1)))
  //   conflictIndex.getConflicts(set("a")) shouldBe IntPrefixSet(Set(0, 1, 2, 3))
  //   conflictIndex.getConflicts(set("b")) shouldBe IntPrefixSet(Set(0, 1))
  // }
  //
  // "A CompactConflictIndex" should "garbageCollect a and b correctly" in {
  //   val kvs = new KeyValueStore()
  //   val conflictIndex = new CompactConflictIndex(kvs.typedConflictIndex[Int]())(
  //     IntPrefixSet.factory
  //   )
  //   conflictIndex.put(0, set("a"))
  //   conflictIndex.put(1, set("b"))
  //   conflictIndex.put(2, set("a"))
  //   conflictIndex.put(3, set("b"))
  //   conflictIndex.garbageCollect(IntPrefixSet(Set(0, 2)))
  //   conflictIndex.getConflicts(set("a")) shouldBe IntPrefixSet(Set(0, 2))
  //   conflictIndex.getConflicts(set("b")) shouldBe IntPrefixSet(Set(0, 1, 2, 3))
  // }
}
