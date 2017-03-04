package io.greenbus.edge.colset

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.{CallMarshaller, SchedulableCallMarshaller}
import io.greenbus.edge.channel2._
import io.greenbus.edge.collection.{BiMultiMap, BiMultiMap$, MapToUniqueValues}

/*
component: update-able subscriptions require the ability to either recognize a table row sub hasn't changed or
go back and get more columns from a later query. need sub sequence in the subscription control / notification

should all data/output keys optionally just be in the manifest???
is transparent access to remote values necessary stage 1, desirable ultimately?

!!! HOLD ON, why is everything not pull?
- instead of publishers, "sources"
- client connects: traditional pseudo-push
- peer relay has list of "sources": greenbus endpoint protocol
- related question: how do stores work? subscribe to all?

local endpoint publisher:
- establish publish to table row set (auth if amqp client)
- establish initial state/values for all table rows
- in peer, publisher registered for table row set
- user layer??:
  - endpoint manifest updated for peer
  - indexes updated according to descriptor


peer subscriber:
- a two-way channel opens, local peer is the subscription provider
- peer subscribes to tables: (endpoint set, endpoint index set, data index set, output index set) <--- THIS IS THE MANIFEST?? (these sets need to distinguish distance?)
- peer subscribes to a set of rows,
  - if local, snapshot is assembled and issued

local subscriber:
- a two-way channel opens, local peer is the subscription provider
- client subscribes at will
  - manifest tables reflect local, peer, and peer-derived
  - subscriber finds out about peer and peer-derived data rows from indexes or endpoint descs
    - !!! peer must infer presence of/path to remote data row from endpointId
    - if not local and not in master peer manifest, must maintain in map of unresolved endpoints
    - if local or remote publisher drops...? need activity state in notifications?
      - remote peer responds to derived sub with either data or an inactive "marker", which is passed on to client

peer source:
- a two-way channel opens, local peer is the subscriber
- subscribe to manifest table(s)
- update our global manifest
- update endpoint -> source path listing
- check unresolved subscriptions, add subs as necessary

local publisher removed:
- update global manifest
- update publisher-owned table rows with inactive flag

peer remote manifest removes endpoint:
- NO, do it when receive sub event on rows from remote peer // update endpoint table rows with inactive flag

subscriber removed:
- if last for remote row key
  - set timeout to GC this sub

OBJECTS:

- peer source channels
- peer source proxies

- per-peer session/rows?

- per-source manifest
- global manifest

- session -> row

- replicated rows
  - subscriber list

- local (pubbed) rows

- peer subscriber proxies
  - row queues

- peer subscriber channels



peer keyspace
source keyspace


is the subscriber-facing table generic, different peer library systems update rows in different ways?
- problem: tables need to be dynamic (create indexes on demand, ask for things that are in peer)
- solution?: user layer modules own tables, get callbacks
- problem: ordering semantics of session transitions would need to be in generic model
  - discontinuities understood?
    - loss of sync, changeover both refinements of discontinuity

solution: routing key, i.e. cassandra partition key

[peer A generic keyspace] <-[naming convention]-> [peer b remote proxy] <-> APP control/merge <->

subscription keyspace model
-> source/endpoint (with endpoint and row indexes)
-> edge logical endpoint model (proto descriptor, metadata, keys, outputs, data types)

 */

trait OutputRequest
trait OutputResponse

/*
I/O layers:

single closeable multi sender/receiver?? what about credit on multiple senders?
credit managing abstraction?
channel link abstraction (exposes credit?)
amqp
 */

trait RemoteSubscribable extends Closeable with CloseObservable {
  def updateSubscription(params: SubscriptionParams)
  def source: Source[SubscriptionNotifications]
}


case class RemotePeerChannels(
                               subscriptionControl: SenderChannel[SubscriptionParams, Boolean],
                               subscriptionReceive: ReceiverChannel[SubscriptionNotifications, Boolean]/*,
                               outputRequests: SenderChannel[OutputRequest, Boolean],
                               outputResponses: ReceiverChannel[OutputResponse, Boolean]*/)

// TODO: "two-(multi-) way channel that combines closeability/closeobservableness and abstracts individual amqp links from actual two-way inter-process comms like websocket
class RemoteSubscribableImpl(eventThread: SchedulableCallMarshaller, channels: RemotePeerChannels) extends RemoteSubscribable {
  def updateSubscription(params: SubscriptionParams): Unit = ???

  def source: Source[SubscriptionNotifications] = ???

  def onClose(): Unit = ???

  def close(): Unit = ???
}

object SetChanges {
  def calc[A](start: Set[A], end: Set[A]): SetChanges[A] = {
    SetChanges(end, end -- start, start -- end)
  }
  def calcOpt[A](start: Set[A], end: Set[A]): Option[SetChanges[A]] = {
    val adds = end -- start
    val removes = start -- end
    if (adds.nonEmpty || removes.nonEmpty) {
      Some(SetChanges(end, end -- start, start -- end))
    } else {
      None
    }
  }
}
case class SetChanges[A](snapshot: Set[A], adds: Set[A], removes: Set[A])

case class ManifestUpdate(routingSet: Option[SetChanges[TypeValue]], indexSet: Option[SetChanges[IndexSpecifier]])

case class PeerSourceEvents(manifestUpdate: Option[ManifestUpdate], sessionNotifications: Seq[StreamEvent])

trait PeerSourceLink {
  def id: PeerSessionId
  def addSubscriptions(params: Map[RoutedTableRowId, Option[TypeValue]]): Unit
  def removedSubscriptions(rows: Set[RoutedTableRowId]): Unit
  def sourced: Set[RoutedTableRowId]
}

trait RemotePeerSourceLink extends CloseObservable {
  def link(): Unit
  def remoteManifest: Source[ManifestUpdate]
}

// TODO: could eliminate direct notifications, which would make peer manifest visible to ui, but need that "rowKey" to not be an endpoint somehow??
class RemotePeerSubscriptionLinkMgr(remoteId: PeerSessionId, subscriptionMgr: RemoteSubscribable) extends /*RemotePeerSourceLink with*/ LazyLogging {

  private val endpointTableRow =  DirectTableRowId(SourcedEndpointPeer.endpointTable, TypeValueConversions.toTypeValue(remoteId))
  private val endpointIndexesTableRow =  DirectTableRowId(SourcedEndpointPeer.endpointIndexTable, TypeValueConversions.toTypeValue(remoteId))
  private val endpointKeyIndexesTableRow =  DirectTableRowId(SourcedEndpointPeer.keyIndexTable, TypeValueConversions.toTypeValue(remoteId))

  //private val endTableSetOpt = Option.empty[TypedSimpleSeqModifiedSetDb]
  private val endTableSet = new UntypedSimpleSeqModifiedSetDb
  private val endIndexesSet = new UntypedSimpleSeqModifiedSetDb
  private val endKeysIndexesSet = new UntypedSimpleSeqModifiedSetDb

  private val distributor = new QueuedDistributor[PeerSourceEvents]
  def events: Source[PeerSourceEvents] = distributor

  def link(): Unit = {
    val endpoints = DirectSetSubscription(endpointTableRow, None)
    val endIndexes = DirectSetSubscription(endpointIndexesTableRow, None)
    val endKeyIndexes = DirectSetSubscription(endpointKeyIndexesTableRow, None)
    val params = SubscriptionParams(Seq(endpoints, endIndexes, endKeyIndexes))

    subscriptionMgr.updateSubscription(params)
    subscriptionMgr.source.bind(handle)
  }

  //def events: Source[PeerSourceEvents]

  //def remoteManifest: Source[ManifestUpdate] = ???

  protected def handle(notifications: SubscriptionNotifications): Unit = {
    val startEndTableSet = endTableSet.current
    val startEndIndexesSet = endIndexesSet.current
    val startKeysIndexesSet = endKeysIndexesSet.current

    notifications.localNotifications.foreach { batch =>
      batch.sets.foreach { set =>
        set.tableRowId match {
          case `endpointTableRow` => endTableSet.observe(set.update)
          case `endpointIndexesTableRow` => endIndexesSet.observe(set.update)
          case `endpointKeyIndexesTableRow` => endKeysIndexesSet.observe(set.update)
          case _ => logger.warn("Unrecognized local set subscription: " + set.tableRowId)
        }
      }
      batch.keyedSets.foreach { set =>
        logger.warn("Unrecognized local keyed set subscription: " + set.tableRowId)
      }
      batch.appendSets.foreach { set =>
        logger.warn("Unrecognized append set subscription: " + set.tableRowId)
      }
    }

    val updatedEndTableSet = endTableSet.current
    val updatedEndIndexesSet = endIndexesSet.current
    val updatedKeysIndexesSet = endKeysIndexesSet.current

    val routingOpt = SetChanges.calcOpt(startEndTableSet, updatedEndTableSet)

    val rowIndexOpt = SetChanges.calcOpt(
      startKeysIndexesSet.flatMap(parseIndexSpecifier),
      updatedKeysIndexesSet.flatMap(parseIndexSpecifier))


    val manifestUpdateOpt = if (routingOpt.nonEmpty || rowIndexOpt.nonEmpty) {
      Some(ManifestUpdate(routingOpt, rowIndexOpt))
    } else {
      None
    }

    ???
    //distributor.push(PeerSourceEvents(manifestUpdateOpt, notifications.sessionNotifications))
  }

  private def parseIndexSpecifier(tv: TypeValue): Option[IndexSpecifier] = {
    PeerManifest.parseIndexSpecifier(tv) match {
      case Right(indexSpecifier) => Some(indexSpecifier)
      case Left(err) => logger.warn(s"$remoteId did not recognize index set value: $tv, error: $err"); None
    }
  }
}

/*
dbs:
links -> synthesizer -> retail

sub => sub mgr -> sourcing (peers, local pubs)

 */

//case class StreamEvent(inactiveFlagSet: Boolean)

trait Subscriber {
  //def appends(directAppends: Seq[(DirectTableRowId, RowStreamEvent)], routedAppends: Seq[(RoutedTableRowId, RowStreamEvent)])
}

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

/*
stream events:

use cases:
- append
  - delta
  - rebase snapshot
  - rebase session (with snapshot)
- row (routing key?) inactive
- backfill

 */
//case class LinkStreamEvent(rowKey: RoutedTableRowId)


trait SetDelta
trait SetSnapshot

case class ModifiedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[TypeValue]) extends SetDelta
case class ModifiedSetSnapshot(sequence: SequencedTypeValue, snapshot: Set[TypeValue]) extends SetSnapshot
case class ModifiedKeyedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[TypeValue], modifies: Set[(TypeValue, TypeValue)]) extends SetDelta
case class ModifiedKeyedSetSnapshot(sequence: SequencedTypeValue, snapshot: Map[TypeValue, TypeValue]) extends SetSnapshot
case class AppendSetValue(sequence: SequencedTypeValue, value: TypeValue)
case class AppendSetDelta(appends: Seq[AppendSetValue]) extends SetDelta with SetSnapshot
//case class AppendSetSnapshot(appends: Seq[AppendSetValue]) extends SetSnapshot


//sealed trait RowStreamEvent
sealed trait AppendEvent
case class StreamDelta(update: SetDelta) extends AppendEvent
case class ResyncSnapshot(snapshot: SetSnapshot) extends AppendEvent
case class ResyncSession(sessionId: PeerSessionId, snapshot: SetSnapshot) extends AppendEvent
//case class Inactive

case class RowId(routingKeyOpt: Option[TypeValue], table: SymbolVal, rowKey: TypeValue) {
  def tableRow: TableRow = TableRow(table, rowKey)
}
case class TableRow(table: SymbolVal, rowKey: TypeValue)

sealed trait StreamEvent
/*case class RoutedRowAppendEvent(key: RoutedTableRowId, appendEvent: RowAppendEvent)*/
case class RowAppendEvent(rowId: RowId, appendEvent: AppendEvent) extends StreamEvent
case class RouteDesynced(routingKey: TypeValue) extends StreamEvent

case class StreamEventBatch(events: Seq[StreamEvent])
case class StreamNotifications(batches: Seq[StreamEventBatch])


/*
Synthesizer,
Retail stream cache,streams
Subscriber retail

 */

class RowSynthTable extends LazyLogging {
  private var routed = Map.empty[TypeValue, Map[TableRow, DbMgr]]
  private var unrouted = Map.empty[TableRow, DbMgr]
  private var sourceToRow = BiMultiMap.empty[PeerSourceLink, RowId] //Map.empty[PeerSourceLink, RowId]

  def handleBatch(sourceLink: PeerSourceLink, events: Seq[StreamEvent]): Seq[StreamEvent] = {
    val results = Vector.newBuilder[StreamEvent]

    events.foreach {
      case ev: RowAppendEvent => {
        val tableRow = ev.rowId.tableRow
        ev.rowId.routingKeyOpt match {
          case None => {
            // TODO: What does this even mean???
            /*unrouted.get(tableRow) match {
              case None => {
                ev.appendEvent match {
                  case resync: ResyncSession =>
                    val (db, events) = DbMgr.build(ev.rowId, sourceLink, resync.sessionId, resync.snapshot)
                    unrouted += (tableRow -> db)
                    results ++= events.map(v => RowAppendEvent(ev.rowId, v))
                  case _ =>
                    logger.warn(s"Initial row event was not resync session: $ev")
                }
              }
              case Some(db) => db.append(sourceLink, ev.appendEvent)
            }*/
          }
          case Some(routingKey) =>
            routed.get(routingKey) match {
              case None => {
                results ++= addRouted(sourceLink, ev, routingKey, Map())
                /*ev.appendEvent match {
                  case resync: ResyncSession =>
                    val (db, events) = DbMgr.build(ev.rowId, sourceLink, resync.sessionId, resync.snapshot)
                    routed += (routingKey -> Map(tableRow -> db))
                    results ++= events.map(v => RowAppendEvent(ev.rowId, v))
                  case _ =>
                    logger.warn(s"Initial row event was not resync session: $ev")
                }*/
              }
              case Some(routingKeyRows) =>
                routingKeyRows.get(tableRow) match {
                  case None => {
                    results ++= addRouted(sourceLink, ev, routingKey, routingKeyRows)
                    /*ev.appendEvent match {
                      case resync: ResyncSession =>
                        val (db, events) = DbMgr.build(ev.rowId, sourceLink, resync.sessionId, resync.snapshot)
                        routed += (routingKey -> (routingKeyRows + (tableRow -> db)))
                        results ++= events.map(v => RowAppendEvent(ev.rowId, v))
                      case _ =>
                        logger.warn(s"Initial row event was not resync session: $ev")
                    }*/
                  }
                  case Some(db) =>
                    db.append(sourceLink, ev.appendEvent)
                }
            }
        }
      }
      case in: RouteDesynced => {
        routed.get(in.routingKey) match {
          case None => Seq()
          case Some(rows) => {
            results ++= rows.toVector.sortBy(_._1).flatMap {
              case (row, db) =>
                val rowId = RowId(Some(in.routingKey), row.table, row.rowKey)
                db.sourceRemoved(sourceLink)
                  .map(append => RowAppendEvent(rowId, append))
            }

            val rowIds = rows.keys.map(tr => RowId(Some(in.routingKey), tr.table, tr.rowKey)).toSet
            sourceToRow = sourceToRow.removeMappings(sourceLink, rowIds)
          }
        }
      }
    }

    results.result()
  }

  def sourceRemoved(sourceLink: PeerSourceLink): Seq[StreamEvent] = {
    val results = sourceToRow.keyToVal.get(sourceLink) match {
      case None => Seq()
      case Some(rowSet) =>
        rowSet.toVector.sorted.flatMap { rowId =>
          lookup(rowId)
            .map(db => db.sourceRemoved(sourceLink).map(apEv => RowAppendEvent(rowId, apEv)))
            .getOrElse(Seq())
        }
    }
    sourceToRow = sourceToRow.removeKey(sourceLink)
    results
  }

  private def lookup(rowId: RowId): Option[DbMgr] = {
    rowId.routingKeyOpt match {
      case None => unrouted.get(rowId.tableRow)
      case Some(routingKey) =>
        routed.get(routingKey).flatMap { map =>
          map.get(rowId.tableRow)
        }
    }
  }

  private def addRouted(sourceLink: PeerSourceLink, ev: RowAppendEvent, routingKey: TypeValue, existingRows: Map[TableRow, DbMgr]): Seq[StreamEvent] = {
    ev.appendEvent match {
      case resync: ResyncSession =>
        val (db, events) = DbMgr.build(ev.rowId, sourceLink, resync.sessionId, resync.snapshot)
        routed += (routingKey -> (existingRows + (ev.rowId.tableRow -> db)))
        sourceToRow += (sourceLink -> ev.rowId)
        events.map(v => RowAppendEvent(ev.rowId, v))
      case _ =>
        logger.warn(s"Initial row event was not resync session: $ev")
        Seq()
    }
  }
}

/*
standby mgrs vs. active mgrs?

 */

object DbMgr {
  def build(rowId: RowId, initSource: PeerSourceLink, initSess: PeerSessionId, snapshot: SetSnapshot): (DbMgr, Seq[AppendEvent]) = {
    val log = SessionRowLog.build(rowId, initSess, snapshot)
    val (events, db) = log.activate()
    val synthesizer = new RowSynthesizerDb(rowId, initSource, initSess, db)
    (synthesizer, events)
  }
}
trait DbMgr {
  def append(source: PeerSourceLink, event: AppendEvent): Seq[AppendEvent]
  def sourceRemoved(source: PeerSourceLink): Seq[AppendEvent]
}

/*
three modes a session can be in:
- active
- standby (not yet active)
- TODO backfill (succeeded, allowing backfill events until a timeout happens or all sources are removed)

"standby" is really "unresolved"? in other words it's not got a clear ordering from the active session

 */
class RowSynthesizerDb(rowId: RowId, initSource: PeerSourceLink, initSess: PeerSessionId, initMgr: SessionRowDb) extends DbMgr with LazyLogging {

  private var activeSession: PeerSessionId = initSess
  private var activeDb: SessionRowDb = initMgr
  //private var activeSources: Set[PeerSourceLink] = Set(initSource)

  //private var standbySessions = Map.empty[PeerSessionId, (SessionModifyRowLog, Set[PeerSourceLink])]
  private var standbySessions = Map.empty[PeerSessionId, SessionRowLog]
  //private var sourceToSession: Map[PeerSourceLink, PeerSessionId] = Map(initSource, initSess)
  //private var sourceMapping: ValueTrackedSetMap[PeerSessionId, PeerSourceLink] = ValueTrackedSetMap((initSess, initSource))
  private var sessionToSources: MapToUniqueValues[PeerSessionId, PeerSourceLink] = MapToUniqueValues((initSess, initSource))
  private def sourceToSession: Map[PeerSourceLink, PeerSessionId] = sessionToSources.valToKey

  def append(source: PeerSourceLink, event: AppendEvent): Seq[AppendEvent] = {
    event match {
      case delta: StreamDelta => {
        sourceToSession.get(source) match {
          case None => logger.warn(s"$rowId got delta for inactive source link $initSource"); Seq()
          case Some(sess) => {
            if (sess == activeSession) {
              activeDb.delta(delta.update).map(StreamDelta)
                .map(v => Seq(v)).getOrElse(Seq())
            } else {
              standbySessions.get(sess) match {
                case None => logger.error(s"$rowId got delta for inactive source link $initSource")
                case Some(log) => log.delta(delta.update)
              }
              Seq()
            }
          }
        }
      }
      case resync: ResyncSnapshot => {
        sourceToSession.get(source) match {
          case None => logger.warn(s"$rowId got snapshot for inactive source link $initSource"); Seq()
          case Some(sess) => {
            if (sess == activeSession) {
              activeDb.resync(resync.snapshot).map(ResyncSnapshot)
                .map(v => Seq(v)).getOrElse(Seq())
            } else {
              standbySessions.get(sess) match {
                case None => logger.error(s"$rowId got delta for inactive source link $initSource")
                case Some(log) => log.resync(resync.snapshot)
              }
              Seq()
            }
          }
        }
      }
      case sessionResync: ResyncSession => {

        // TODO: session succession logic by actually reading peer session id
        val prevSessOpt = sourceToSession.get(source)
        sessionToSources = sessionToSources + (sessionResync.sessionId -> source)

        if (sessionResync.sessionId == activeSession) {

          // gc sessions with no sources
          prevSessOpt.foreach { prevSess =>
            if (!sessionToSources.keyToVal.contains(prevSess)) {
              standbySessions -= prevSess
            }
          }

          activeDb.resync(sessionResync.snapshot).map(ResyncSnapshot)
            .map(v => Seq(v)).getOrElse(Seq())

        } else {

          standbySessions.get(sessionResync.sessionId) match {
            case None => {
              val log = SessionRowLog.build(rowId, sessionResync.sessionId, sessionResync.snapshot)
              standbySessions += (sessionResync.sessionId -> log)
            }
            case Some(log) => log.resync(sessionResync.snapshot)
          }

          activeSessionChangeCheck()
        }
      }
    }
  }

  // TODO: need to mark succeeded sessions to prevent immediately reverting to a prev session propped up by a source who is just delayed
  private def activeSessionChangeCheck(): Seq[AppendEvent] = {
    if (!sessionToSources.keyToVal.contains(activeSession)) {

      if (standbySessions.keySet.size == 1) {
        val (nowActiveSession, log) = standbySessions.head
        standbySessions -= nowActiveSession
        activeSession = nowActiveSession
        val (events, db) = log.activate()
        activeDb = db
        events

      } else {
        Seq()
      }

    } else {
      Seq()
    }
  }

  def sourceRemoved(source: PeerSourceLink): Seq[AppendEvent] = {

    val prevSessOpt = sourceToSession.get(source)
    sessionToSources = sessionToSources.remove(source)

    prevSessOpt match {
      case None => Seq()
      case Some(prevSess) =>
        if (prevSess == activeSession) {
          activeSessionChangeCheck()
        } else {
          if (!sessionToSources.keyToVal.contains(prevSess)) {
            standbySessions -= prevSess
          }
          Seq()
        }
    }
  }

}

object SessionRowLog {
  def build(rowId: RowId, sessionId: PeerSessionId, init: SetSnapshot): SessionRowLog = {
    init match {
      case snap: ModifiedSetSnapshot => new SessionModifyRowLog(rowId, sessionId, snap)
    }
  }
}
trait SessionRowLog {
  def activate(): (Seq[AppendEvent], SessionRowDb)
  def delta(delta: SetDelta): Unit
  def resync(snapshot: SetSnapshot): Unit
}

trait SessionRowDb {
  def delta(delta: SetDelta): Option[SetDelta]
  def resync(snapshot: SetSnapshot): Option[SetSnapshot]
}

/*object SessionModifyRowLog {
  def build(rowId: RowId, sessionId: PeerSessionId, init: ModifiedSetSnapshot): SessionModifyRowLog = {
    new SessionModifyRowLog(rowId, sessionId, init)
  }
}*/
class SessionModifyRowLog(rowId: RowId, sessionId: PeerSessionId, init: ModifiedSetSnapshot) extends SessionRowLog with LazyLogging {

  private var sequence: SequencedTypeValue = init.sequence
  private var current: Set[TypeValue] = init.snapshot

  def activate(): (Seq[AppendEvent], SessionModifyRowDbMgr) = {
    val snap = ModifiedSetSnapshot(sequence, current)
    (Seq(ResyncSession(sessionId, snap)),
      new SessionModifyRowDbMgr(rowId, sessionId, snap))
  }

  def delta(delta: SetDelta): Unit = {
    delta match {
      case d: ModifiedSetDelta =>
        if (sequence.precedes(d.sequence)) {
          sequence = d.sequence
          current = (current -- d.removes) ++ d.adds
        } else {
          logger.debug(s"Set delta unsequenced for $rowId, session $sessionId, current: $sequence, delta: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set delta event for $rowId, session $sessionId: $delta")
        None
    }
  }

  def resync(snapshot: SetSnapshot): Unit = {
    snapshot match {
      case d: ModifiedSetSnapshot =>
        if (sequence.isLessThan(d.sequence)) {
          sequence = d.sequence
          current = d.snapshot
        } else {
          logger.debug(s"Set snapshot unsequenced for $rowId, session $sessionId, current: $sequence, snap: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set snapshot event for $rowId, session $sessionId: $snapshot")
        None
    }
  }
}

class SessionModifyRowDbMgr(rowId: RowId, sessionId: PeerSessionId, init: ModifiedSetSnapshot) extends SessionRowDb with LazyLogging {

  private var sequence: SequencedTypeValue = init.sequence
  private var current: Set[TypeValue] = init.snapshot

  def delta(delta: SetDelta): Option[SetDelta] = {
    delta match {
      case d: ModifiedSetDelta => {
        if (sequence.precedes(d.sequence)) {
          sequence = d.sequence
          current = (current -- d.removes) ++ d.adds
          Some(delta)
        } else {
          logger.debug(s"Set delta unsequenced for $rowId, session $sessionId, current: $sequence, delta: ${d.sequence}")
          None
        }
      }
      case _ =>
        logger.warn(s"Unrecognized set delta event for $rowId, session $sessionId: $delta")
        None
    }
  }

  def resync(snapshot: SetSnapshot): Option[SetSnapshot] = {
    snapshot match {
      case d: ModifiedSetSnapshot =>
        if (sequence.isLessThan(d.sequence)) {
          sequence = d.sequence
          current = d.snapshot
          Some(snapshot)
        } else {
          logger.debug(s"Set snapshot unsequenced for $rowId, session $sessionId, current: $sequence, snap: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set snapshot event for $rowId, session $sessionId: $snapshot")
        None
    }
  }
}
class SessionAppendRowDbMgr {

  def resync(link: PeerSourceLink, snapshot: SetSnapshot) = ???
}



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

//case class SynthesizeResult(appends: Seq[(RoutedTableRowId, GenRowAppend)])

/*sealed trait EndpointId
case class UuidEndpointId(uuid: UUID) extends EndpointId
case class SessionNamedEndpointId(name: String, session: PeerSessionId) extends EndpointId*/

/*object IndexSpecifier {
  //val typeDesc: TypeDesc = TupleDesc(Seq(AnyDesc, OptionDesc(IndexableTypeDesc)))

}*/
case class IndexSpecifier(key: TypeValue, value: Option[IndexableTypeValue])
//case class PeerManifest(routingKeySet: Set[TypeValue], endpointIndexSet: Map[IndexSpecifier, Set[RoutedTableRowId]], keyIndexSet: Map[IndexSpecifier, Set[RoutedTableRowId]])


//case class PeerManifest(routingKeySet: Set[TypeValue], indexSet: Set[IndexSpecifier])

object PeerManifest {

  def eitherSomeIsRightOrNone[L, R](vOpt: Option[Either[L, R]]): Either[L, Option[R]] = {
    vOpt.map(_.map(r => Some(r))).getOrElse(Right(Option.empty[R]))
  }

  def parseIndexableTypeValue(tv: TypeValue): Either[String, IndexableTypeValue] = {
    tv match {
      case v: IndexableTypeValue => Right(v)
      case _ => Left(s"Unrecognized indexable type value: $tv")
    }
  }

  def parseIndexSpecifier(tv: TypeValue): Either[String, IndexSpecifier] = {
    tv match {
      case TupleVal(seq) =>
        if (seq.size >= 2) {
          val key = seq(0)
          seq(1) match {
            case v: OptionVal =>
              eitherSomeIsRightOrNone(v.element.map(parseIndexableTypeValue))
                  .map(vOpt => IndexSpecifier(key, vOpt))
            case _ => Left(s"Unrecognized index type: $tv")
          }
        } else {
          Left(s"Unrecognized index type: $tv")
        }
      case _ => Left(s"Unrecognized index type: $tv")
    }
  }
}
case class PeerManifest(routingKeySet: Set[TypeValue], indexSet: Set[IndexSpecifier])


class SourceLinksManifest[Link] {

  private var routingMap = BiMultiMap.empty[TypeValue, Link]
  private var indexMap = BiMultiMap.empty[IndexSpecifier, Link]

  def routingKeys: Map[TypeValue, Set[Link]] = routingMap.keyToVal
  def indexes: Map[IndexSpecifier, Set[Link]] = indexMap.keyToVal

  def handleUpdate(link: Link, manifest: PeerManifest): Unit = {
    routingMap = routingMap.reverseAdd(link, manifest.routingKeySet)
    indexMap = indexMap.reverseAdd(link, manifest.indexSet)
  }

  def linkRemoved(link: Link): Unit = {
    routingMap = routingMap.removeValue(link)
    indexMap = indexMap.removeValue(link)
  }
}

class LocalManifest[Publisher] {

}

class PeerMasterManifest {


}

/*

is inactive a client subscriber concept?


Publish:

- routing/partition key
- row values
- row indexes

effects:
- update local manifest with routing as locally sourced, and indexes
- update master manifest / push to direct database with routing/indexes
- push to (routed? as self?) database

update active on inactive subs?
local set keeps updating current value when no subs, append has a window?
could local pubs be in a local zone that is a source link for the routed table, bridged only when a sub happens?
  - is this a better transition to pull-based clients

Subscribe:

- direct subs
- routed subs

procedure:
- foreach row
  - if already active
    - tag that row with sub
    - get cached according to query
    - setup sub for later logs
  - if not active and in unresolved set
    - add to sub table
    - publish inactive flag notification
  -  if not active and not in unresolved set
    - do manifest lookup
      - if unresolved, see above
      - if routing path exists
        - pick a path
        - add to link subscription

Manifest update:
- merge scala tables
- update direct manifest tables
- if routingKey removed, new session may be active

 */

/*

Peer manifest
Local manifest
Already-active peer link subs, and their targets
Unresolved subscriptions


data (cache) table: routed_row -> row_db
subscription table: routed_row -> set[subscriber]
unresolved row set: set[routed_row] OR set[routingKey -> set[row]]

 */

case class PeerLinkEntry()


/*
Synthesizer,
Retail stream cache,streams
Subscriber retail

 */

class PeerThing {

  private val sourcedManifest = new SourceLinksManifest[PeerLinkEntry]

  //private val sourcedTable = new SourcedDataTable
  //private val directTable = new DirectDataTable

  //private var

  private var subscriptions = Map.empty[RoutedTableRowId, Set[Subscription]]
  private var unresolvedSubs = Map.empty[RoutedTableRowId, Set[Subscription]]
  //private var subscribers = Map.empty[Subscriber, Set[RoutedTableRowId]]

  /*
    update manifest
      - resolve resolveable unresolved subscriptions
      - apply to manifest subscribers' logs

    get events yielded by applying this source's events
      - apply to retail caches
      - apply to subscribers' logs

    manifest component just another subscriber, do event before commit to subscribers?
   */
  def peerSourceEvents(link: PeerSourceLink, events: Seq[StreamEvent]): Unit = {

  }
  /*def peerSourceEvents(link: PeerSourceLink, events: PeerSourceEvents): Unit = {

  }*/

  def subscriptionsRegistered(subscriber: Subscriber, params: SubscriptionParams): Unit = {

  }

}


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

/*class RemotePeerProxyImpl(eventThread: SchedulableCallMarshaller, remoteId: PeerSessionId, channels: RemotePeerChannels) extends Closeable with CloseObservable {

  protected def init(): Unit = {
    val endpoints = ModifiedSetSubscription(SourcedEndpointPeer.endpointTable, TypeValueConversions.toTypeValue(remoteId))
    val endIndexes = ModifiedSetSubscription(SourcedEndpointPeer.endpointIndexTable, TypeValueConversions.toTypeValue(remoteId))
    val endKeyIndexes = ModifiedSetSubscription(SourcedEndpointPeer.keyIndexTable, TypeValueConversions.toTypeValue(remoteId))
    val params = SubscriptionParams(Seq(endpoints, endIndexes, endKeyIndexes))

    channels.subscriptionControl.send(params)
  }

  def close(): Unit = ???

  def onClose(): Unit = ???
}*/


/*trait RemotePeerProxy {
  //def subscribeManifest()

  def manifest: Source[ManifestUpdate]

}*/

object SourcedEndpointPeer {
  val tablePrefix = "sep"
  val endpointTable = SymbolVal(s"$tablePrefix.endpoints")
  val endpointIndexTable = SymbolVal(s"$tablePrefix.endpointIndexes")
  val keyIndexTable = SymbolVal(s"$tablePrefix.keyIndexes")

}

class SourcedEndpointPeer {

  //private val keySpace: Database = null

  def onLocalPublisherOpened(): Unit = ???
  def onLocalPublisherClosed(): Unit = ???

  //def onRemotePeerOpened(proxy: RemotePeerProxy): Unit = ???
  def onRemotePeerClosed(): Unit = ???

  //def onPeerRemoteManifestUpdate()
  def onRemotePeerNotifications(notifications: SubscriptionNotifications): Unit = ???

  def onPeerSubscriber(): Unit = ???
  def onLocalSubscriber(): Unit = ???

}