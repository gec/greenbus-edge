package io.greenbus.edge.colset

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.{CallMarshaller, SchedulableCallMarshaller}
import io.greenbus.edge.channel2._
import io.greenbus.edge.collection.{BiMultiMap, MapToUniqueValues}

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
case class RouteUnresolved(routingKey: TypeValue) extends StreamEvent

case class StreamEventBatch(events: Seq[StreamEvent])
case class StreamNotifications(batches: Seq[StreamEventBatch])

case class IndexSpecifier(key: TypeValue, value: Option[IndexableTypeValue])

case class RowSubscriptionParams(rowId: RowId, columnQuery: Option[SessionColumnQuery])
case class StreamSubscriptionParams(rows: Seq[RowSubscriptionParams])



/*
Synthesizer,
Retail stream cache,streams
Subscriber retail

 */

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



trait RetailRowQueue {

  def handle(append: AppendEvent): Unit

  def dequeue(): Seq[AppendEvent]
}

object RetailRowLog {
  def build(rowId: RowId, sessionId: PeerSessionId, init: SetSnapshot): RetailRowLog = {
    ???
  }
}
trait RetailRowLog {

  def handle(append: AppendEvent): Unit

  def query(): Seq[AppendEvent]
}


class RetailCacheTable extends LazyLogging {
  private var rows = Map.empty[TypeValue, Map[TableRow, RetailRowLog]]
  private var unrouted = Map.empty[TableRow, RetailRowLog]

  def handleBatch(events: Seq[StreamEvent]): Unit = {
    events.foreach {
      case ev: RowAppendEvent => {
        lookup(ev.rowId) match {
          case None => //
            ev.rowId.routingKeyOpt match {
              case None => ???
              case Some(routingKey) => addRouted(ev, routingKey, rows.getOrElse(routingKey, Map()))
            }
          case Some(log) => log.handle(ev.appendEvent)
        }
      }
      case ev: RouteUnresolved =>
    }
  }

  private def lookup(rowId: RowId): Option[RetailRowLog] = {
    rowId.routingKeyOpt match {
      case None => unrouted.get(rowId.tableRow)
      case Some(routingKey) =>
        rows.get(routingKey).flatMap { map =>
          map.get(rowId.tableRow)
        }
    }
  }


  private def addRouted(ev: RowAppendEvent, routingKey: TypeValue, existingRows: Map[TableRow, RetailRowLog]): Unit = {
    ev.appendEvent match {
      case resync: ResyncSession =>
        val log = RetailRowLog.build(ev.rowId, resync.sessionId, resync.snapshot)
        rows += (routingKey -> (existingRows + (ev.rowId.tableRow -> log)))
      case _ =>
        logger.warn(s"Initial row event was not resync session: $ev")
    }
  }
}


/*
Synthesizer,
Retail stream cache,streams
Subscriber retail

 */

/*trait PeerSourceLink {
  def id: PeerSessionId
  def addSubscriptions(params: Map[RoutedTableRowId, Option[TypeValue]]): Unit
  def removedSubscriptions(rows: Set[RoutedTableRowId]): Unit
  def sourced: Set[RoutedTableRowId]
}*/
trait PeerSourceLink {
  def setSubscriptions(rows: Set[(RowId, Option[SessionColumnQuery])]): Unit
  //def sourcedRoutes: Set[TypeValue]
  //def sourcedRows: Set[RowId]
}

object SourceMgr {
  val tablePrefix = "__manifest"

  def peerRouteRow(peerId: PeerSessionId): RowId = {
    RowId(Some(TypeValueConversions.toTypeValue(peerId)), SymbolVal(s"$tablePrefix"), SymbolVal("routes"))
  }
  def peerIndexRow(peerId: PeerSessionId): RowId = {
    RowId(Some(TypeValueConversions.toTypeValue(peerId)), SymbolVal(s"$tablePrefix"), SymbolVal("indexes"))
  }

  def manifestRows(peerId: PeerSessionId): Set[RowId] = {
    Set(peerRouteRow(peerId), peerIndexRow(peerId))
  }
}
class SourceMgr(peerId: PeerSessionId, source: PeerSourceLink) {

  private val manifestKeys: Set[RowId] = SourceMgr.manifestRows(peerId)

  def init(): Unit = {
    source.setSubscriptions(manifestKeys.map(k => (k, None)))
  }

}

/*

handles:
- sub appears: resolve routes, enqueue unresolved or add subscription to a source
- sub disappears: unmark source subs, remove if unnecessary
- source appears: add to manifest rows to observe
- source manifest update: resolve unresolved, add/remove known routes
- source disappears: check for now-unsourced routes, switch to a different source or emit unresolved.
!!!NOTE: conflict with the synthesizer's concept of unresolved

manifest rows -> source (manifest)


 */
/*class SourcingMgr {
  private var sourceToSubs = Map.empty[PeerSourceLink, Set[RowId]]
  //private var manifestRowsToSource

  private var routesToSource = Map.empty[TypeValue, PeerSourceLink]

  def manifestRows: Set[RowId]

  def handleSynthesizedEvents(events: Seq[StreamEvent]): Unit = {

  }

}*/

trait SubscriptionTarget {
  //def queues: Map[TypeValue, Map[TableRow, RetailRowQueue]]
  def handleBatch(events: Seq[StreamEvent])
}

class SubMgr(target: SubscriptionTarget) {

  /*def handleBatch(events: Seq[StreamEvent]): Unit = {
    target.handleBatch(events)
  }*/
  def paramsUpdate(params: StreamSubscriptionParams): (Seq[RowSubscriptionParams], Set[RowId]) = {

  }
}

/*trait RowSubscription {
  def queue: RetailRowQueue
}*/

object PeerStreamEngine {

}
class PeerStreamEngine {

  private val synthesizer = new SynthesizerTable

  private val retailCacheTable = new RetailCacheTable

  private val sourcedManifest = new SourceLinksManifest[PeerLinkEntry]

  //private var subscriptions = Map.empty[RoutedTableRowId, Set[Subscription]]
  //private var unresolvedSubs = Map.empty[RoutedTableRowId, Set[Subscription]]
  //private var subscribers = Map.empty[Subscriber, Set[RoutedTableRowId]]

  // MANIFEST
  private var routesToSource = Map.empty[TypeValue, PeerSourceLink]


  // SUB MGMT
  private var activeSubscriptions = Map.empty[TypeValue, Map[TableRow, (Set[SubscriptionTarget], Set[PeerSourceLink])]]
  private var activeForSub = Map.empty[SubscriptionTarget, Set[RowId]]
  private var activeForSource = Map.empty[PeerSourceLink, Set[RowId]]

  private var unresolvedSubRoutes = Map.empty[TypeValue, Map[SubscriptionTarget, Set[TableRow]]]
  private var unresolvedForSub = Map.empty[SubscriptionTarget, Map[TypeValue, Map[TableRow, Option[SessionColumnQuery]]]]

  //private var subscriptions = Map.empty[RowId, Set[Subscription]]
  //private var subscriptionsRouted = Map.empty[TypeValue, Map[TableRow, Set[SubMgr]]]

  //private var targetToMgr = Map.empty[SubscriptionTarget, SubMgr]

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
    val emitted = synthesizer.handleBatch(link, events)
    handleSynthesizedEvents(emitted)
  }

  private def handleSynthesizedEvents(events: Seq[StreamEvent]): Unit = {


    retailCacheTable.handleBatch(events)
  }

  def sourceConnected(link: PeerSourceLink): Unit = {

  }

  def sourceDisconnected(link: PeerSourceLink): Unit = {
    val emitted = synthesizer.sourceRemoved(link)
    handleSynthesizedEvents(emitted)
  }

  private def handleSubAdds(subMgr: SubMgr, adds: Map[RowId, Option[SessionColumnQuery]]): Unit = {
    adds.foreach {
      case (rowId, queryOpt) =>
        rowId.routingKeyOpt match {
          case None => ???
          case Some(routingKey) => {


            routesToSource.get(routingKey) match {
              case None =>
              case Some(source) => {

              }
            }
          }
        }
    }
  }

  private def handleSubRemoves(subMgr: SubMgr, removes: Set[RowId]): Unit = {

  }

  // - sub appears: resolve routes, enqueue unresolved or add subscription to a source
  def subscriptionsRegistered(subscriber: SubscriptionTarget, params: StreamSubscriptionParams): Unit = {

    val paramMap: Map[RowId, Option[SessionColumnQuery]] = params.rows.map(p => (p.rowId, p.columnQuery)).toMap

    val active: Set[RowId] = activeForSub.getOrElse(subscriber, Set())
    val unresolved = unresolvedForSub.getOrElse(subscriber, Set())
    val unresolvedMap: Map[RowId, Option[SessionColumnQuery]] = unresolved.flatMap { case (route, rowQueries) => rowQueries.map { case (tr, optQuery) => (RowId(Some(route), tr.table, tr.rowKey), optQuery) } }.toMap

    val activeRemoves = active -- paramMap.keySet

    val unresolvedRemoves = unresolvedMap.keySet -- paramMap.keySet




    /*val (removes, adds) = activeForSub.get(subscriber) match {
      case None => (Set(), paramMap)
      case Some(rows) =>
    }*/

    /*val subMgr = targetToMgr.getOrElse(subscriber, {
      val subMgr = new SubMgr(subscriber)
      targetToMgr += (subscriber -> subMgr)
      subMgr
    })

    val (requests, removes) = subMgr.paramsUpdate(params)
    handleSubRemoves(subMgr, removes)*/

    /*targetToMgr.get(subscriber) match {
      case None =>
        val subMgr = new SubMgr(subscriber)
        targetToMgr += (subscriber -> subMgr)
        val (requests, removes) = subMgr.paramsUpdate(params)
      case Some(subMgr) =>
        val (requests, removes) = subMgr.paramsUpdate(params)
    }*/
  }

}

/*
Highest level

App Entry
- Peer
  - Subscription protocol engine
  - Output forwarding engine
- Link management / I/O

 */



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