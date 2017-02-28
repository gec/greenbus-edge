package io.greenbus.edge.colset

import io.greenbus.edge.{CallMarshaller, SchedulableCallMarshaller}
import io.greenbus.edge.channel2._

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


// TODO: "two-(multi-) way channel that combines closeability/closeobservableness and abstracts individual amqp links from actual two-way inter-process comms like websocket
class RemoteSubscribableImpl(eventThread: SchedulableCallMarshaller, channels: RemotePeerChannels) extends RemoteSubscribable {
  def updateSubscription(params: SubscriptionParams): Unit = ???

  def source: Source[SubscriptionNotifications] = ???

  def onClose(): Unit = ???

  def close(): Unit = ???
}

case class ManifestUpdate()


class RemotePeerSubscriptionLinkMgr(remoteId: PeerSessionId, subscriptionMgr: RemoteSubscribable) {

  private val endpointTableRow =  TableRowId(SourcedEndpointPeer.endpointTable, TypeValueConversions.toTypeValue(remoteId))
  private val endpointIndexesTableRow =  TableRowId(SourcedEndpointPeer.endpointIndexTable, TypeValueConversions.toTypeValue(remoteId))
  private val endpointKeyIndexesTableRow =  TableRowId(SourcedEndpointPeer.keyIndexTable, TypeValueConversions.toTypeValue(remoteId))

  def link(): Unit = {
    val endpoints = ModifiedSetSubscription(endpointTableRow)
    val endIndexes = ModifiedSetSubscription(endpointIndexesTableRow)
    val endKeyIndexes = ModifiedSetSubscription(endpointKeyIndexesTableRow)
    val params = SubscriptionParams(Seq(endpoints, endIndexes, endKeyIndexes))

    subscriptionMgr.updateSubscription(params)
    subscriptionMgr.source.bind(handle)
  }

  def remoteManifest: Source[ManifestUpdate] = ???

  protected def handle(notifications: SubscriptionNotifications): Unit = {
    notifications.localNotifications.foreach { batch =>
      batch.sets.foreach { set =>
        set.tableRowId match {
          case `endpointTableRow` => set.update
        }
      }
    }
  }
}

case class RemotePeerChannels(
                               subscriptionControl: SenderChannel[SubscriptionParams, Boolean],
                               subscriptionReceive: ReceiverChannel[SubscriptionNotifications, Boolean]/*,
                               outputRequests: SenderChannel[OutputRequest, Boolean],
                               outputResponses: ReceiverChannel[OutputResponse, Boolean]*/)


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


trait RemotePeerProxy {
  //def subscribeManifest()

  def manifest: Source[ManifestUpdate]

}

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

  def onRemotePeerOpened(proxy: RemotePeerProxy): Unit = ???
  def onRemotePeerClosed(): Unit = ???

  //def onPeerRemoteManifestUpdate()
  def onRemotePeerNotifications(notifications: SubscriptionNotifications): Unit = ???

  def onPeerSubscriber(): Unit = ???
  def onLocalSubscriber(): Unit = ???

}