package io.greenbus.edge.colset

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.{CallMarshaller, SchedulableCallMarshaller}
import io.greenbus.edge.channel2._
import io.greenbus.edge.collection.ValueTrackedSetMap

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

case class ManifestUpdate()


trait RemotePeerSourceLink extends CloseObservable {
  def link(): Unit
  def remoteManifest: Source[ManifestUpdate]
}

class RemotePeerSubscriptionLinkMgr(remoteId: PeerSessionId, subscriptionMgr: RemoteSubscribable) extends /*RemotePeerSourceLink with*/ LazyLogging {

  private val endpointTableRow =  DirectTableRowId(SourcedEndpointPeer.endpointTable, TypeValueConversions.toTypeValue(remoteId))
  private val endpointIndexesTableRow =  DirectTableRowId(SourcedEndpointPeer.endpointIndexTable, TypeValueConversions.toTypeValue(remoteId))
  private val endpointKeyIndexesTableRow =  DirectTableRowId(SourcedEndpointPeer.keyIndexTable, TypeValueConversions.toTypeValue(remoteId))

  //private val endTableSetOpt = Option.empty[TypedSimpleSeqModifiedSetDb]
  private val endTableSet = new UntypedSimpleSeqModifiedSetDb
  private val endIndexesSet = new UntypedSimpleSeqModifiedSetDb
  private val endKeysIndexesSet = new UntypedSimpleSeqModifiedSetDb

  def link(): Unit = {
    val endpoints = DirectSetSubscription(endpointTableRow, None)
    val endIndexes = DirectSetSubscription(endpointIndexesTableRow, None)
    val endKeyIndexes = DirectSetSubscription(endpointKeyIndexesTableRow, None)
    val params = SubscriptionParams(Seq(endpoints, endIndexes, endKeyIndexes))

    subscriptionMgr.updateSubscription(params)
    subscriptionMgr.source.bind(handle)
  }

  def remoteManifest: Source[ManifestUpdate] = ???

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

  }
}

/*
dbs:
links -> synthesizer -> retail

sub => sub mgr -> sourcing (peers, local pubs)

 */

trait Subscriber {
  def observe()
}

trait GenRowDb
trait GenRowAppend

trait MarkedRowView {
  def dequeue()
}

class MultiSourcedRowDb {
  private var link = Map.empty[RemotePeerSourceLink, GenRowDb]
}

class SessionedRowDb(id: RoutedTableRowId) {
  private var active: PeerSessionId = null
  private var sessions = Map.empty[PeerSessionId, MultiSourcedRowDb]
}


/*
events:
- link added
  - link added and newly active session/row
- link removed
  - link removed and session now empty
- updates
- subscriber added
- subscriber removed

external results:
- appends

 */

/*



 */
class SynthesizedTable {
  private var rowSynthesizers = Map.empty[RoutedTableRowId, SessionedRowDb]
  private var linkToRowSessions = Map.empty[RemotePeerSourceLink, Set[(RoutedTableRowId, SessionedRowDb)]]

  private var retailRows = Map.empty[RoutedTableRowId, GenRowDb]

  /*private var subscriptions = Map.empty[TableRowId, Set[Subscription]]
  private var subscribers = Map.empty[Subscriber, Set[TableRowId]]*/

  def sourceEvents(source: RemotePeerSourceLink, sessionNotifications: Seq[SessionNotificationSequence]): Unit = {

  }

  def sourceRemoved(source: RemotePeerSourceLink): Unit = {

  }

  def rowInactive(row: RoutedTableRowId): Unit = {

  }

  /*def subscriptionsRegistered(subscriber: Subscriber, subscriptions: Seq[GenericSetSubscription]): Unit = {

  }*/


  //private var db = Map.empty[TableRowId, Map[PeerSessionId, Map[RemotePeerSourceLink, GenRowDb]]]

  //def linkRemoved(link: RemotePeerSourceLink): SynthesizeResult
}

case class SynthesizeResult(appends: Seq[(RoutedTableRowId, GenRowAppend)])

/*sealed trait EndpointId
case class UuidEndpointId(uuid: UUID) extends EndpointId
case class SessionNamedEndpointId(name: String, session: PeerSessionId) extends EndpointId*/

case class IndexSpecifier(key: TypeValue, value: Option[IndexableTypeValue])
//case class PeerManifest(routingKeySet: Set[TypeValue], endpointIndexSet: Map[IndexSpecifier, Set[RoutedTableRowId]], keyIndexSet: Map[IndexSpecifier, Set[RoutedTableRowId]])
case class PeerManifest(routingKeySet: Set[TypeValue], indexSet: Set[IndexSpecifier])


class SourceLinksManifest[Link] {

  /*private var routingMap = Map.empty[TypeValue, Set[Link]]
  private var indexMap = Map.empty[TypeValue, Set[Link]]

  private var linkToRoute = Map.empty[]*/


  private var routingMap = ValueTrackedSetMap.empty[TypeValue, Link]
  private var indexMap = ValueTrackedSetMap.empty[IndexSpecifier, Link]

  def routingKeys: Map[TypeValue, Set[Link]] = routingMap.keyToVal
  def indexes: Map[IndexSpecifier, Set[Link]] = indexMap.keyToVal

  def handleUpdate(link: Link, manifest: PeerManifest): Unit = {
    routingMap = routingMap.reverseAdd(link, manifest.routingKeySet)
    indexMap = indexMap.reverseAdd(link, manifest.indexSet)
  }

  def linkRemoved(link: Link): Unit = {
    routingMap = routingMap.remove(link)
    indexMap = indexMap.remove(link)
  }
}

class LocalManifest[Publisher] {

}

class PeerMasterManifest {


}

/*

Peer manifest
Local manifest
Already-active peer link subs, and their targets
Unresolved subscriptions

- Routing


 */
/*
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

 */

class PeerThing {
  private val routedTable = new SynthesizedTable

  //private var

  private var subscriptions = Map.empty[RoutedTableRowId, Set[Subscription]]
  private var unresolvedSubs = Map.empty[RoutedTableRowId, Set[Subscription]]
  private var subscribers = Map.empty[Subscriber, Set[RoutedTableRowId]]

  def subscriptionsRegistered(subscriber: Subscriber, subscriptions: Seq[RoutedSetSubscription]): Unit = {

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

  private val keySpace: Database = null

  def onLocalPublisherOpened(): Unit = ???
  def onLocalPublisherClosed(): Unit = ???

  //def onRemotePeerOpened(proxy: RemotePeerProxy): Unit = ???
  def onRemotePeerClosed(): Unit = ???

  //def onPeerRemoteManifestUpdate()
  def onRemotePeerNotifications(notifications: SubscriptionNotifications): Unit = ???

  def onPeerSubscriber(): Unit = ???
  def onLocalSubscriber(): Unit = ???

}