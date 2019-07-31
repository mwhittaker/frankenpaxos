package frankenpaxos.util

// A CompactSet is an add-only set of values that can _sometimes_ be compacted
// to use O(1) space. For example, imagine we have the following set of natural
// numbers:
//
//     {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 100, 200}
//
// Representing this set as a plain old Set[Int] would require us to store 16
// integers. However, we can represent this set more compactly using an
// integer-valued watermark and a set like this:
//
//     watermark: 15, values: {100, 200}
//
// A compacted set with watermark `w` and values `v` represents the set of
// natural numbers `{x | 0 <= x < v} union v`. Of course, this is just one
// example of a CompactSet. You could also implement a CompactSet set of
// integers as set of disjoint ranges; you could implement a CompactSet of
// integer-valued tuples using a set of integer CompactSets; etc.
//
// Note that a CompactSet is not _guaranteed_ to use O(1) space. Even in the
// example above, we can construct sets that do not compact to O(1) space. A
// CompactSet provides only a best-effort attempt at compaction.
trait CompactSet[Self <: CompactSet[Self]] {
  // The type of element stored in the CompactSet.
  type T

  // Add an element to the set, returning whether the element already existed
  // in the set.
  def add(x: T): Boolean

  // Return whether an element exists in the set.
  def contains(x: T): Boolean

  // Compute the set difference of two compact sets.
  def diff(other: Self): Self

  // Materialize the compact set as an actual Set.
  def materialize(): Set[T]
}
