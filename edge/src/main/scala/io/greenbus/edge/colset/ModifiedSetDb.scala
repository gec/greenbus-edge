package io.greenbus.edge.colset



//case class ModifiedSetUpdate(sequence: TypeValue, snapshot: Option[Set[TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])

trait ModifiedSetDb {
  def observe(update: ModifiedSetUpdate)
  //def latestSequence: TypeValue
  def current: Set[TypeValue]
}

class SimpleSequencedModifiedSet[A](start: Long, initial: Set[A]) {
  private var currentSequence: Long = start
  private var state: Set[A] = initial

  def current: Set[A] = state

  def snapshotUpdate(sequence: Long, snapshot: Set[A]): Boolean = {
    if (sequence > currentSequence) {
      currentSequence = sequence
      state = snapshot
      true
    } else {
      false
    }
  }

  def diffUpdate(sequence: Long, removes: Set[A], adds: Set[A]): Boolean = {
    if (sequence == currentSequence + 1) {
      currentSequence = sequence
      state = (state -- removes) ++ adds
      true
    } else {
      false
    }
  }
}

class UntypedSimpleSeqModifiedSetDb(start: Long, initial: Set[TypeValue]) extends ModifiedSetDb {

  private val impl = new SimpleSequencedModifiedSet[TypeValue](start, initial)

  def observe(update: ModifiedSetUpdate): Boolean = {
    update.sequence match {
      case UInt64Val(seq) =>
        update.snapshot match {
          case None => impl.diffUpdate(seq, update.removes, update.adds)
          case Some(snap) => impl.snapshotUpdate(seq, snap)
        }
      case _ => false
    }
  }

  def current: Set[TypeValue] = impl.current

}