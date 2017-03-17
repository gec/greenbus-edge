/**
 * Copyright 2011-2017 Green Energy Corp.
 *
 * Licensed to Green Energy Corp (www.greenenergycorp.com) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. Green Energy
 * Corp licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.greenbus.edge.colset.gateway

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.CallMarshaller
import io.greenbus.edge.colset._
import io.greenbus.edge.flow.Sender

import scala.util.{ Failure, Success, Try }

trait SetStateType[Full, Diff] {

  sealed trait State
  case object UnboundUninit extends State
  case class Unbound(current: Full) extends State
  case class BoundUninit(snapSender: Sender[SetSnapshot, Boolean], deltaSender: Sender[SetDelta, Boolean]) extends State
  case class Bound(deltaSender: Sender[SetDelta, Boolean], current: Full) extends State
}

object MapDiff {
  def calculate[A, B](next: Map[A, B], prev: Map[A, B]): (Set[A], Set[(A, B)], Set[(A, B)]) = {
    val removed = prev.keySet -- next.keySet

    val added = Set.newBuilder[(A, B)]
    val modified = Set.newBuilder[(A, B)]
    next.foreach {
      case (k, v) =>
        prev.get(k) match {
          case None => added += (k -> v)
          case Some(lastV) =>
            if (lastV != v) {
              modified += (k -> v)
            }
        }
    }

    (removed, added.result(), modified.result())
  }

}
case class KeyedSetDiff[A, B](snapshot: Map[A, B], removed: Set[A], added: Set[(A, B)], modified: Set[(A, B)])
case class MapDiff(removes: Set[TypeValue], adds: Set[(TypeValue, TypeValue)], modifies: Set[(TypeValue, TypeValue)])

object KeyedSetSink extends SetStateType[Map[TypeValue, TypeValue], MapDiff] {

  def diff(next: Map[TypeValue, TypeValue], prev: Map[TypeValue, TypeValue]): MapDiff = {
    val (removed, added, modified) = MapDiff.calculate(next, prev)
    MapDiff(removed, added, modified)
  }

  def toDelta(seq: Long, diff: MapDiff): SetDelta = {
    ModifiedKeyedSetDelta(Int64Val(seq), diff.removes, diff.adds, diff.modifies)
  }

  def toSnapshot(seq: Long, current: Map[TypeValue, TypeValue]): SetSnapshot = {
    ModifiedKeyedSetSnapshot(Int64Val(seq), current)
  }
}
class KeyedSetSink extends KeyedSetEventSink with BindableRowMgr with LazyLogging {
  import KeyedSetSink._

  private var sequence: Long = 0
  private var state: State = UnboundUninit

  def update(set: Map[TypeValue, TypeValue]): Unit = {
    state = state match {
      case UnboundUninit => Unbound(set)
      case s: Unbound =>
        sequence += 1
        s.copy(current = set)
      case s: BoundUninit =>
        val seq = sequence
        s.snapSender.send(toSnapshot(seq, set), _ => {})
        Bound(s.deltaSender, set)
      case s: Bound =>
        sequence += 1
        val seq = sequence
        s.deltaSender.send(toDelta(seq, diff(set, s.current)), _ => {})
        s.copy(current = set)
    }
  }

  def bind(snapshot: Sender[SetSnapshot, Boolean], deltas: Sender[SetDelta, Boolean]): Unit = {
    state = state match {
      case UnboundUninit => BoundUninit(snapshot, deltas)
      case s: Unbound => {
        snapshot.send(toSnapshot(sequence, s.current), _ => ())
        Bound(deltas, s.current)
      }
      case _ => state
    }
  }

  def unbind(): Unit = {
    state = state match {
      case s: BoundUninit => UnboundUninit
      case s: Bound => Unbound(s.current)
      case _ => state
    }
  }
}

case class SetDiff(removed: Set[TypeValue], added: Set[TypeValue])
object SetSink extends SetStateType[Set[TypeValue], SetDiff] {

  def diff(next: Set[TypeValue], prev: Set[TypeValue]): SetDiff = {
    val added = next -- prev
    val removed = prev -- next
    SetDiff(removed = removed, added = added)
  }

  def toDelta(seq: Long, diff: SetDiff): SetDelta = {
    ModifiedSetDelta(Int64Val(seq), diff.removed, diff.added)
  }

  def toSnapshot(seq: Long, current: Set[TypeValue]): SetSnapshot = {
    ModifiedSetSnapshot(Int64Val(seq), current)
  }
}
class SetSink extends SetEventSink with BindableRowMgr with LazyLogging {
  import SetSink._

  private var sequence: Long = 0
  private var state: State = UnboundUninit

  def update(set: Set[TypeValue]): Unit = {
    state = state match {
      case UnboundUninit => Unbound(set)
      case s: Unbound =>
        sequence += 1
        s.copy(current = set)
      case s: BoundUninit =>
        val seq = sequence
        s.snapSender.send(toSnapshot(seq, set), _ => {})
        Bound(s.deltaSender, set)
      case s: Bound =>
        sequence += 1
        val seq = sequence
        s.deltaSender.send(toDelta(seq, diff(set, s.current)), _ => {})
        s.copy(current = set)
    }
  }

  def bind(snapshot: Sender[SetSnapshot, Boolean], deltas: Sender[SetDelta, Boolean]): Unit = {
    state = state match {
      case UnboundUninit => BoundUninit(snapshot, deltas)
      case s: Unbound => {
        snapshot.send(toSnapshot(sequence, s.current), _ => ())
        Bound(deltas, s.current)
      }
      case _ => state
    }
  }

  def unbind(): Unit = {
    state = state match {
      case s: BoundUninit => UnboundUninit
      case s: Bound => Unbound(s.current)
      case _ => state
    }
  }
}

object AppendSink {

  sealed trait State
  case object UnboundUninit extends State
  case class Unbound(buffer: Vector[(Long, TypeValue)]) extends State {
    assert(buffer.nonEmpty)
  }
  case class UnboundConfirmed(lastConfirmed: (Long, TypeValue), buffer: Vector[(Long, TypeValue)]) extends State
  case class BoundUninit(snapSender: Sender[SetSnapshot, Boolean], deltaSender: Sender[SetDelta, Boolean]) extends State
  case class Bound(deltaSender: Sender[SetDelta, Boolean], pending: Vector[(Long, TypeValue)]) extends State
  case class BoundConfirmed(deltaSender: Sender[SetDelta, Boolean], lastConfirmed: (Long, TypeValue), pending: Vector[(Long, TypeValue)]) extends State

}
class AppendSink(maxBuffered: Int, eventThread: CallMarshaller) extends AppendEventSink with BindableRowMgr with LazyLogging {
  import AppendSink._

  private var sequence: Long = 0
  private var state: State = UnboundUninit

  def bind(snapshot: Sender[SetSnapshot, Boolean], deltas: Sender[SetDelta, Boolean]): Unit = {
    state match {
      case UnboundUninit => state = BoundUninit(snapshot, deltas)
      case s: Unbound =>
        val pending = s.buffer
        publishSnapshot(snapshot, pending)
        state = Bound(deltas, pending)
      case s: UnboundConfirmed =>
        val pending = Vector(s.lastConfirmed) ++ s.buffer
        publishSnapshot(snapshot, pending)
        state = BoundConfirmed(deltas, s.lastConfirmed, pending)
      case _ => throw new IllegalStateException("Row already bound")
    }

  }

  def unbind(): Unit = {
    state = state match {
      case s: BoundUninit => UnboundUninit
      case s: Bound => Unbound(s.pending)
      case s: BoundConfirmed => UnboundConfirmed(s.lastConfirmed, s.pending)
      case _ => state
    }
  }

  def confirmed(seq: Long): Unit = {
    eventThread.marshal {
      state match {
        case s: Unbound => {
          val (confirmed, stillBuffered) = s.buffer.span(_._1 <= seq)
          confirmed.lastOption match {
            case None => state = s.copy(buffer = stillBuffered)
            case Some(last) => state = UnboundConfirmed(last, stillBuffered)
          }
        }
        case s: UnboundConfirmed => {
          val (confirmed, stillBuffered) = s.buffer.span(_._1 <= seq)
          confirmed.lastOption match {
            case None => state = s.copy(buffer = stillBuffered)
            case Some(last) => state = s.copy(lastConfirmed = last, buffer = stillBuffered)
          }
        }
        case s: Bound => {
          val (confirmed, stillPending) = s.pending.span(_._1 <= seq)
          confirmed.lastOption match {
            case None => state = s.copy(pending = stillPending)
            case Some(last) => state = BoundConfirmed(s.deltaSender, last, stillPending)
          }
        }
        case s: BoundConfirmed => {
          val (confirmed, stillPending) = s.pending.span(_._1 <= seq)
          confirmed.lastOption match {
            case None => state = s.copy(pending = stillPending)
            case Some(last) => state = s.copy(lastConfirmed = last, pending = stillPending)
          }
        }
        case _ =>
      }
    }
  }

  def append(values: TypeValue*): Unit = {
    eventThread.marshal {
      if (values.nonEmpty) {
        val vseq = Range(0, values.size).map(_ + sequence).zip(values).toVector
        sequence += values.size
        state match {
          case UnboundUninit => state = Unbound(vseq)
          case s: Unbound => state = s.copy(buffer = s.buffer ++ vseq)
          case s: UnboundConfirmed => state = s.copy(buffer = s.buffer ++ vseq)
          case s: BoundUninit =>
            publishSnapshot(s.snapSender, vseq)
            state = Bound(s.deltaSender, pending = vseq)
          case s: Bound =>
            publish(s.deltaSender, vseq)
            state = s.copy(pending = s.pending ++ vseq)
          case s: BoundConfirmed =>
            publish(s.deltaSender, vseq)
            state = s.copy(pending = s.pending ++ vseq)
        }
      }
    }
  }

  private def publishSnapshot(publisher: Sender[SetSnapshot, Boolean], snapshot: Seq[(Long, TypeValue)]): Unit = {
    if (snapshot.nonEmpty) {

      val last = snapshot.last._1

      def handleResult(result: Try[Boolean]): Unit = {
        result match {
          case Success(_) => confirmed(last)
          case Failure(ex) =>
            logger.debug(s"AppendLog send failure: " + ex)
        }
      }

      publisher.send(toAppendSeq(snapshot), handleResult)
    }
  }

  private def publish(publisher: Sender[SetDelta, Boolean], updates: Seq[(Long, TypeValue)]): Unit = {
    if (updates.nonEmpty) {

      val last = updates.last._1

      def handleResult(result: Try[Boolean]): Unit = {
        result match {
          case Success(_) => confirmed(last)
          case Failure(ex) =>
            logger.debug(s"AppendLog send failure: " + ex)
        }
      }

      publisher.send(toAppendSeq(updates), handleResult)
    }
  }

  private def toAppendSeq(updates: Seq[(Long, TypeValue)]): AppendSetSequence = {
    val values = updates.map {
      case (seq, v) => AppendSetValue(Int64Val(seq), v)
    }

    AppendSetSequence(values)
  }
}
