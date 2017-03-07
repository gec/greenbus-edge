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

/*class RowSynthesizer {
  private var db = Map.empty[TableRowId, SessionedRowDb]
  //private var db = Map.empty[TableRowId, Map[PeerSessionId, Map[RemotePeerSourceLink, GenRowDb]]]

  //def linkRemoved(link: RemotePeerSourceLink): SynthesizeResult
}*/

/*class PeerRowMgr {

  def peerSourceLinkOpened(peer: RemotePeerSourceLink): Unit = {

  }

  def subscriberOpened(): Unit = {

  }
}*/

/*object SessionRowDbMgr {
  def build(link: PeerSourceLink, sessionId: PeerSessionId, rowKey: RoutedTableRowId, update: SetUpdate): Either[String, SessionRowDbMgr] = {
    RowDb.build(rowKey, update).map(db => new SessionRowDbMgr(rowKey, sessionId, link, db))
  }
}
class SessionRowDbMgr(row: RoutedTableRowId, sessionId: PeerSessionId, origLink: PeerSourceLink, db: RowDb) {
  //private var linkMap: Map[PeerSourceLink, RowDb] = Map(orig)

  //def process()

}

object MultiSessionRowDbMgr {

  def build(link: PeerSourceLink, sessionId: PeerSessionId, rowKey: RoutedTableRowId, update: SetUpdate): Either[String, MultiSessionRowDbMgr] = {
    SessionRowDbMgr.build(link, sessionId, rowKey, update).map(db => new MultiSessionRowDbMgr(rowKey, (sessionId, db)))
  }
}

class MultiSessionRowDbMgr(id: RoutedTableRowId, orig: (PeerSessionId, SessionRowDbMgr)) {
  private var active: PeerSessionId = orig._1
  private var sessions: Map[PeerSessionId, SessionRowDbMgr] = Map(orig)

  def sessionMap: Map[PeerSessionId, SessionRowDbMgr] = sessions

  def process(session: PeerSessionId, link: PeerSourceLink, update: SetUpdate) = {

  }

  def sourceRemoved(link: PeerSourceLink) = {

  }
}*/

/*
events:
- link added
  - link added and newly active session/row
- link removed
  - link removed and session now empty
- updates
  - link transitions to different session
- subscriber added
- subscriber removed

external results:
- appends

 */
//case class DataTableResult()

/*object SourcedDataTable {

  sealed trait RowProcessResult

}
class SourcedDataTable {
  private var rowSynthesizers = Map.empty[RoutedTableRowId, MultiSessionRowDbMgr]
  private var linkToRowSessions = Map.empty[PeerSourceLink, Set[(RoutedTableRowId, MultiSessionRowDbMgr)]]

  private var retailRows = Map.empty[RoutedTableRowId, GenRowDb]

  /*private var subscriptions = Map.empty[TableRowId, Set[Subscription]]
  private var subscribers = Map.empty[Subscriber, Set[TableRowId]]*/

  private def process(source: PeerSourceLink, sessId: PeerSessionId, rowId: RoutedTableRowId, setUpdate: SetUpdate) = {
    rowSynthesizers.get(rowId) match {
      case None =>
        MultiSessionRowDbMgr.build(source, sessId, rowId, setUpdate) match {
          case Left(err) =>
          case Right(db) =>
            rowSynthesizers += (rowId -> db)
            linkToRowSessions += (source -> (rowId, sessId))
        }
      case Some(sessRowDb) => sessRowDb.process(sessId, setUpdate)
    }
  }

  def sourceEvents(source: PeerSourceLink, sessionNotifications: Seq[SessionNotificationSequence]): DataTableResult = {

    sessionNotifications.flatMap { sessNot =>
      val sessId: PeerSessionId = sessNot.session
      sessNot.batches.flatMap { batch =>
        batch.notifications.flatMap { notification =>
          val rowId = notification.tableRowId
          notification.update.flatMap { setUpdate =>

            /*rowSynthesizers.get(rowId) match {
              case None =>
                SessionedRowDb.build(source, sessId, rowId, setUpdate) match {
                  case Left(err) =>
                  case Right(db) =>
                    rowSynthesizers += (rowId -> db)
                    linkToRowSessions += (source -> (rowId, sessId))
                }
              case Some(sessRowDb) => sessRowDb.process(sessId, setUpdate)
            }*/
            ???

            /*rowSynthesizers.get(rowId).flatMap { sessRow =>

              sessRow.handle(sessId, setUpdate)

              ???
            }*/
          }
        }
      }

    }

    ???
  }

  def sourceRemoved(source: PeerSourceLink): DataTableResult = {
    ???
  }

  def rowUnsubscribed(row: RoutedTableRowId): DataTableResult = {
    ???
  }

  /*def subscriptionsRegistered(subscriber: Subscriber, subscriptions: Seq[GenericSetSubscription]): Unit = {

  }*/


  //private var db = Map.empty[TableRowId, Map[PeerSessionId, Map[RemotePeerSourceLink, GenRowDb]]]

  //def linkRemoved(link: RemotePeerSourceLink): SynthesizeResult
}*/

/*
trait GenRowDb
trait GenRowAppend
*/

/*trait MarkedRowView {
  def dequeue()
}*/

/*

case class ModifiedSetUpdate(sequence: TypeValue, snapshot: Option[Set[TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue]) extends SetUpdate
case class ModifiedKeyedSetUpdate(sequence: TypeValue, snapshot: Option[Map[TypeValue, TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue]) extends SetUpdate
case class AppendSetUpdate(sequence: TypeValue, value: TypeValue) extends SetUpdate
 */
/*object RowDb {
  def build(id: RoutedTableRowId, update: SetUpdate): Either[String, RowDb] = {
    update match {
      case mod: ModifiedSetUpdate => {
        mod.snapshot match {
          case None => Left("no snapshot in original set update")
          case Some(snap) => Right(new ModSetRowDb(snap))
        }
      }
      case keyed: ModifiedKeyedSetUpdate => {
        keyed.snapshot match {
          case None => Left("no snapshot in original keyed set update")
          case Some(snap) => Right(new KeyedSetRowDb(snap))
        }
      }
      case mod: ModifiedSetUpdate => {
        mod.snapshot match {
          case None => Left("no snapshot in original append set update")
          case Some(snap) => Right(new AppendSetRowDb(snap))
        }
      }
    }
  }
}
trait RowDb {
  def process(update: SetUpdate)
}

class ModSetRowDb(orig: Set[TypeValue]) extends RowDb {
  def process(update: SetUpdate) = {

  }
}
class KeyedSetRowDb(orig: Map[TypeValue, TypeValue]) extends RowDb {
  def process(update: SetUpdate) = {

  }
}
class AppendSetRowDb(orig: Set[TypeValue]) extends RowDb {
  def process(update: SetUpdate) = {

  }
}*/ 