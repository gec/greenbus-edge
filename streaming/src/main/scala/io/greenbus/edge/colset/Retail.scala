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
package io.greenbus.edge.colset

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

/*
trait RetailRowQueue {

  def handle(append: AppendEvent): Unit

  def dequeue(): Seq[AppendEvent]
}


object RetailRowCache {
  def build(rowId: RowId, sessionId: PeerSessionId, init: SetSnapshot): Option[RetailRowCache] = {
    init match {
      case snap: ModifiedSetSnapshot => Some(new ModifiedSetRetailRowCache(rowId, sessionId, snap))
      case snap: ModifiedKeyedSetSnapshot => Some(new KeyedModifiedSetRetailRowCache(rowId, sessionId, snap))
      case snap: AppendSetSequence =>
        snap.appends.lastOption.map(last => new AppendSetRetailRowCache(rowId, sessionId, last))
    }
  }
}

trait RetailRowCache {

  def handle(append: AppendEvent): Unit

  def sync(): Seq[AppendEvent]
  //def query(): Seq[AppendEvent]
}

class AppendSetRetailRowCache(rowId: RowId, initSess: PeerSessionId, last: AppendSetValue) extends RetailRowCache with LazyLogging {

  private var session: PeerSessionId = initSess
  private var latest: AppendSetValue = last

  private def handleAppends(appends: Seq[AppendSetValue]): Unit = {
    appends.foreach { append =>
      if (latest.sequence.isLessThan(append.sequence).contains(true)) {
        latest = append
      }
    }
  }

  def handle(append: AppendEvent): Unit = {

    append match {
      case StreamDelta(update) => {
        update match {
          case d: AppendSetSequence => handleAppends(d.appends)
          case _ => logger.error(s"Incorrect delta type for $rowId")
        }
      }
      case ResyncSnapshot(setSnap) => {
        setSnap match {
          case snap: AppendSetSequence => handleAppends(snap.appends)
          case _ => logger.error(s"Incorrect snapshot type for $rowId")
        }
      }
      case ResyncSession(sess, setSnap) => {
        setSnap match {
          case snap: AppendSetSequence =>
            snap.appends.headOption.foreach { head =>
              session = sess
              latest = head
              handleAppends(snap.appends.tail)
            }
          case _ => logger.error(s"Incorrect snapshot type for $rowId")
        }
      }
    }
  }

  def sync(): Seq[AppendEvent] = {
    Seq(ResyncSession(session, AppendSetSequence(Seq(latest))))
  }
}

class AppendSetRetailRowQueue(rowId: RowId, initSess: PeerSessionId, startLast: AppendSetValue) extends RetailRowQueue with LazyLogging {

  private val buffer: mutable.ArrayBuffer[AppendEvent] = mutable.ArrayBuffer[AppendEvent](ResyncSession(initSess, AppendSetSequence(Seq(startLast))))

  def handle(append: AppendEvent): Unit = {
    buffer += append
  }

  def dequeue(): Seq[AppendEvent] = {
    val results = buffer.toVector
    buffer.clear()
    results
  }
}

class ModifiedSetRetailRowCache(rowId: RowId, initSess: PeerSessionId, initSnap: ModifiedSetSnapshot) extends RetailRowCache with LazyLogging {
  private var log: SessionModifyRowLog = new SessionModifyRowLog(rowId, initSess, initSnap)

  def handle(append: AppendEvent): Unit = {
    append match {
      case StreamDelta(update) => {
        log.handleDelta(update)
      }
      case ResyncSnapshot(setSnap) => {
        log.handleResync(setSnap)
      }
      case ResyncSession(sess, setSnap) => {
        setSnap match {
          case snap: ModifiedSetSnapshot =>
            log = new SessionModifyRowLog(rowId, sess, snap)
          case _ => logger.error(s"Incorrect snapshot type for $rowId: $setSnap")
        }
      }
    }
  }

  def sync(): Seq[AppendEvent] = {
    log.resyncEvents
  }
}

class ModifiedSetRetailRowQueue(rowId: RowId, initSess: PeerSessionId, initSnap: ModifiedSetSnapshot) extends RetailRowQueue with LazyLogging {

  private var sequence: SequencedTypeValue = initSnap.sequence
  private var sessionResync: Option[PeerSessionId] = Some(initSess)
  private var snapshotRebase: Option[ModifiedSetSnapshot] = Some(initSnap)
  private val deltas = mutable.ArrayBuffer.empty[ModifiedSetDelta]

  def handle(append: AppendEvent): Unit = {
    append match {
      case StreamDelta(update) => {
        update match {
          case d: ModifiedSetDelta => {
            if (sequence.precedes(d.sequence)) {
              snapshotRebase match {
                case None =>
                  deltas += d
                  sequence = d.sequence
                case Some(state) =>
                  snapshotRebase = Some(ModifiedSetSnapshot(d.sequence, (state.snapshot -- d.removes) ++ d.adds))
                  sequence = d.sequence
              }
            } else {
              logger.warn(s"Retail sequence $rowId saw unsequenced delta: $d")
            }
          }
          case _ => logger.error(s"Incorrect delta type for $rowId: $update")
        }
      }
      case ResyncSnapshot(setSnap) => {
        setSnap match {
          case snap: ModifiedSetSnapshot => {
            deltas.clear()
            snapshotRebase = Some(snap)
            sequence = snap.sequence
          }
          case _ => logger.error(s"Incorrect snapshot type for $rowId: $setSnap")
        }
      }
      case ResyncSession(sess, setSnap) => {
        setSnap match {
          case snap: ModifiedSetSnapshot => {
            deltas.clear()
            sessionResync = Some(sess)
            snapshotRebase = Some(snap)
            sequence = snap.sequence
          }
          case _ => logger.error(s"Incorrect snapshot type for $rowId: $setSnap")
        }
      }
    }
  }

  def dequeue(): Seq[AppendEvent] = {
    sessionResync match {
      case None => {
        snapshotRebase match {
          case None => {
            val results = deltas.map(StreamDelta).toVector
            deltas.clear()
            results
          }
          case Some(snap) => {
            val results = Seq(ResyncSnapshot(snap))
            snapshotRebase = None
            results
          }
        }
      }
      case Some(session) => {
        val results = snapshotRebase.map(snap => ResyncSession(session, snap)).toVector
        snapshotRebase = None
        sessionResync = None
        results
      }
    }
  }
}

class KeyedModifiedSetRetailRowCache(rowId: RowId, initSess: PeerSessionId, initSnap: ModifiedKeyedSetSnapshot) extends RetailRowCache with LazyLogging {
  private var log: SessionKeyedModifyRowLog = new SessionKeyedModifyRowLog(rowId, initSess, initSnap)

  def handle(append: AppendEvent): Unit = {
    append match {
      case StreamDelta(update) => {
        log.handleDelta(update)
      }
      case ResyncSnapshot(setSnap) => {
        log.handleResync(setSnap)
      }
      case ResyncSession(sess, setSnap) => {
        setSnap match {
          case snap: ModifiedKeyedSetSnapshot =>
            log = new SessionKeyedModifyRowLog(rowId, sess, snap)
          case _ => logger.error(s"Incorrect snapshot type for $rowId")
        }
      }
    }
  }

  def snapshot: Map[TypeValue, TypeValue] = {
    log.snapshot
  }

  def sync(): Seq[AppendEvent] = {
    log.resyncEvents
  }
}

class KeyedModifiedSetRetailRowQueue(rowId: RowId, initSess: PeerSessionId, initSnap: ModifiedKeyedSetSnapshot) extends RetailRowQueue with LazyLogging {

  private var sequence: SequencedTypeValue = initSnap.sequence
  private var sessionResync: Option[PeerSessionId] = Some(initSess)
  private var snapshotRebase: Option[ModifiedKeyedSetSnapshot] = Some(initSnap)
  private val deltas = mutable.ArrayBuffer.empty[ModifiedKeyedSetDelta]

  def handle(append: AppendEvent): Unit = {
    append match {
      case StreamDelta(update) => {
        update match {
          case d: ModifiedKeyedSetDelta => {
            if (sequence.precedes(d.sequence)) {
              snapshotRebase match {
                case None =>
                  deltas += d
                  sequence = d.sequence
                case Some(state) =>
                  snapshotRebase = Some(ModifiedKeyedSetSnapshot(d.sequence, (state.snapshot -- d.removes) ++ d.adds ++ d.modifies))
                  sequence = d.sequence
              }
            } else {
              logger.warn(s"Retail sequence $rowId saw unsequenced delta: $d")
            }
          }
          case _ => logger.error(s"Incorrect delta type for $rowId")
        }
      }
      case ResyncSnapshot(setSnap) => {
        setSnap match {
          case snap: ModifiedKeyedSetSnapshot => {
            deltas.clear()
            snapshotRebase = Some(snap)
            sequence = snap.sequence
          }
          case _ => logger.error(s"Incorrect snapshot type for $rowId")
        }
      }
      case ResyncSession(sess, setSnap) => {
        setSnap match {
          case snap: ModifiedKeyedSetSnapshot => {
            deltas.clear()
            sessionResync = Some(sess)
            snapshotRebase = Some(snap)
            sequence = snap.sequence
          }
          case _ => logger.error(s"Incorrect snapshot type for $rowId")
        }
      }
    }
  }

  def dequeue(): Seq[AppendEvent] = {
    sessionResync match {
      case None => {
        snapshotRebase match {
          case None => {
            val results = deltas.map(StreamDelta).toVector
            deltas.clear()
            results
          }
          case Some(snap) => {
            val results = Seq(ResyncSnapshot(snap))
            snapshotRebase = None
            results
          }
        }
      }
      case Some(session) => {
        val results = snapshotRebase.map(snap => ResyncSession(session, snap)).toVector
        snapshotRebase = None
        sessionResync = None
        results
      }
    }
  }
}
*/
