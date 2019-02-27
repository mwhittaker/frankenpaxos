package frankenpaxos.fastmultipaxos

import collection.mutable

// A Log[A] represents a log of entries of type A with a potentially infinite
// tail consisting of some fixed value. For example, this is a Log[Char]:
//
//   +---+---+---+---+---+---+---+---+---+---+---+-
//   | a | b |   | c |   | d | d | d | d | d | ....
//   +---+---+---+---+---+---+---+---+---+---+---+-
//     0   1   2   3   4   5   6   7   8   9
//
// It has a, b, and c in slots 0, 1, and 3; slots 2 and 4 are both empty; and
// there is an infinite tail of the character d starting in slot 5. We can
// create such a log with the following code.
//
//   new Log[Int]
//     .put(0, a)
//     .put(1, b)
//     .put(3, c)
//     .putTail(5, d)
//
// We can also remove entries from a log and set the tail multiple times.
// Consider the following code.
//
//   // Same as before.
//   new Log[Int]
//     .put(0, a)
//     .put(1, b)
//     .put(3, c)
//     .putTail(5, d)
//     // New stuff.
//     .remove(0)
//     .putTail(3, e)
//     .putTail(7, f)
//     .remove(4)
//
// It creates the following log.
//
//   +---+---+---+---+---+---+---+---+---+---+---+-
//   |   | b |   | e |   | e | e | f | f | f | ....
//   +---+---+---+---+---+---+---+---+---+---+---+-
//     0   1   2   3   4   5   6   7   8   9
class Log[A] {
  // The finite prefix of the log. map(i) stores the value in slot i.
  private[this] var map: mutable.SortedMap[Int, A] = mutable.SortedMap()

  // The infinite tail of the log. If tail is None, then the log does not have
  // an infinite tail. If tail = Some(tailSlot, tailValue), then the log has an
  // infinite tail of value `tailValue` starting in slot `tailSlot`. Note that
  // the largest slot in `map` is always less than `tailSlot`. Thus, there is
  // never a slot that is in `map` and covered by `tail`.
  private var tail: Option[(Int, A)] = None

  override def toString(): String = {
    map.toString() + " with tail " + tail.toString()
  }

  def get(slot: Int): Option[A] = {
    tail match {
      case Some((tailSlot, tailValue)) =>
        if (slot < tailSlot) map.get(slot) else Some(tailValue)
      case None => map.get(slot)
    }
  }

  def put(slot: Int, value: A): Log[A] = {
    tail match {
      case Some((tailSlot, tailValue)) =>
        if (slot < tailSlot) {
          // If we insert an element before the tail, then life is easy. We
          // don't have to modify the tail at all.
          map += (slot -> value)
        } else {
          // Otherwise, if we insert an element into the tail, then we have to
          // move the elements in the tail before `slot` into map. For example,
          // if we have the following log
          //
          //   +---+---+---+---+---+---+---+---+---+---+---+-
          //   | a | b |   | c |   | d | d | d | d | d | ....
          //   +---+---+---+---+---+---+---+---+---+---+---+-
          //     0   1   2   3   4   5   6   7   8   9
          //
          // and call put(7, e), then we have to move the d's in slot 5 and 6
          // from the tail into `map`. If we implemented Log[A] with multiple
          // tails, then we wouldn't have to do this, but for our simple
          // single-tailed implementation, we do.
          for (i <- tailSlot until slot) {
            map += (i -> tailValue)
          }
          map += (slot -> value)
          tail = Some(slot + 1, tailValue)
        }
      case None =>
        // If there is no tail, again, our life is easy.
        map += (slot -> value)
    }
    this
  }

  def putTail(slot: Int, value: A): Log[A] = {
    tail match {
      case Some((tailSlot, tailValue)) =>
        if (slot <= tailSlot) {
          // If we create a new tail before the existing tail, then we
          // completeley overwrite the existing tail. The one thing we have to
          // be careful of is deleting entries from `map` that are now covered
          // by the new tail.

          // TODO(mwhittaker): I think we can implement this more efficiently.
          // retain is probably iterating unncessarily over the whole map.
          map = map.retain((s, _) => s < slot)
          tail = Some((slot, value))
        } else {
          // Otherwise, the new tail partially overlaps the old tail. We have
          // to make sure to move over the non-overwritten tail elements to
          // `map`.
          for (i <- tailSlot until slot) {
            map += (i -> tailValue)
          }
          tail = Some(slot, value)
        }
      case None =>
        // If we don't have a tail, then we create a new tail and make sure to
        // remove any values covered by the tail.
        //
        // TODO(mwhittaker): See above.
        map = map.retain((s, _) => s < slot)
        tail = Some((slot, value))
    }
    this
  }

  def remove(slot: Int): Option[A] = {
    // TODO(mwhittaker): Implement if needed.
    ???
  }
}
