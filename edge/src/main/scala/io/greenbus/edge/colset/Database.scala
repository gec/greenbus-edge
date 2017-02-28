package io.greenbus.edge.colset

import java.util.UUID



/*

users:
- client
- server (proxy)
- server (table provider)

1. library-managed subscriber proxies

publish -> concrete update -> synthetic view update -> foreach subscriber -> update sub proxy -> merge/send ->

n global updates
merge to m / flush
m sub proxy updates
merge to p / flush or wait for send credit

def onPublish(batch) {

}

2. user-managed subscriber proxies

 */

/*
SESSION MANAGEMENT

- back to back-propagating a demand for ordering, but with order relations between sessions being an immutable fact that can be re-published
- communicate back a list of unresolved session orderings OR just your active session set
- sessions of distance 1 are always synced by timeouts

Peer Node:
- current session uuid

- Session Manager
  - Active sessions -> set[table/row]
  - Legacy sessions -> set[table/row]
  - Undetermined sessions -> set[table/row]

- Each incoming link n:
  - Remote:
    - Their session
    - Their set of replicated active sessions, with distance
      - clock goes here?
    - Set of replicated session orderings??? bound by what?
  - Local:
    - Set of undetermined session orderings


 */

//trait FieldDesc
trait TypeDesc
trait TypeValue

trait ModifiedKeyedSetTable {
  def rowKeyType: TypeDesc
}

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

- per-source manifest
- global manifest

- local (pubbed) rows
- replicated rows
  - subscriber list

- peer subscriber proxies
  - row queues

- peer subscriber channels



peer keyspace
source keyspace


subscription keyspace model
-> source/endpoint (with endpoint and row indexes)
-> edge logical endpoint model (proto descriptor, metadata, keys, outputs, data types)

 */

/*
table ontology:

- sourced tables (end descs, data keys, 'real data', can be inactive if underlying source goes away)
- synthetic tables (endpoint sets, index sets, inactive makes no sense, generally?)
- dynamic typed tables, sourced/synth (active sets, value type is per-row)

is it really "synthetic" or just "immediate", it can't "go away" who cares whether it's "real"

we need to escape to user layer to map descriptors to synthetic tables, is this unfortunate, i.e. we
should think about putting the concepts of endpoints and indexes into the DB layer, or on the other hand
does it allow us to create even more synthetic types

 */

class SourcedEndpointPeer {

  private val keySpace: Database = null

  def onLocalPublisherOpened(): Unit = ???
  def onLocalPublisherClosed(): Unit = ???

  //def onPeerSource(): Unit = ???

  def onPeerSubscriber(): Unit = ???
  def onLocalSubscriber(): Unit = ???

}

trait Row {

}

sealed trait RowType
case object ModifyRowType extends RowType

// TODO: how do we abstract away session ids
trait Database {

  // create

  def createModifiedSet(rowType: RowType, rowKeyDesc: TypeDesc): Unit

  def createKeyedModifiedSet(rowType: RowType, rowKeyDesc: TypeDesc, elementKey: TypeDesc): Unit

  def createAppendSet(rowType: RowType, rowKeyDesc: TypeDesc, columnKey: TypeDesc): Unit

  // for active sets
  def createDynamic()

}

trait PeerPullChannel {
  def updateSubscriptionSet(modSubs: Seq[ModifiedSetSubscription], appendSubs: Seq[LocalAppendSetSubscription])
}

class PeerPullProxy {


}

// TODO: subscribe direct vs. subscribe indirect (with session engine)
trait DatabaseView {

  //def subscribeToSetModify(table: String, rowKey: TypeValue, notify: () => Unit)
  //def subscribeToSetAppend(table: String, rowKey: TypeValue, columnQuery: TypeValue, notify: () => Unit)

  def subscribe(modSubs: Seq[ModifiedSetSubscription], appendSubs: Seq[LocalAppendSetSubscription], notify: () => Unit): Subscription

}


trait SubscriberProxy {
}




trait Subscription {
  def dequeue(): SubscriptionNotifications
  def close(): Unit
}

case class SessionColumnQuery(sessionId: SessId, sequence: TypeValue)

case class ModifiedSetSubscription(table: String, rowKey: TypeValue)
case class LocalAppendSetSubscription(table: String, rowKey: TypeValue, columnQuery: Option[TypeValue])
//case class AppendSetSubscription(table: String, rowKey: TypeValue, columnQuery: Option[SessionColumnQuery])

// TODO: should sequence just be long, should sessions be built in? where does session-awareness go in the layering?

// TODO: IS THE WRITER TIMED OUT? can we do this within session notifications? no; how do we then handle disappearance of client publisher while peer still okay
// TODO: pull table/rowKey out, value update is seq/payload, notification is: (table/row, option(value update), option(inactive))
/*case class ModifiedSetNotification(table: String, rowKey: TypeValue, sequence: TypeValue, snapshot: Option[Set[TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class ModifiedKeyedSetNotification(table: String, rowKey: TypeValue, sequence: TypeValue, snapshot: Option[Map[TypeValue, TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class AppendSetNotification(table: String, rowKey: TypeValue, sequence: TypeValue, value: TypeValue)*/

case class TableRowId(table: String, rowKey: TypeValue)
case class ModifiedSetUpdate(sequence: TypeValue, snapshot: Option[Set[TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class ModifiedKeyedSetUpdate(sequence: TypeValue, snapshot: Option[Map[TypeValue, TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class AppendSetUpdate(sequence: TypeValue, value: TypeValue)

case class ModifiedSetNotification(tableRowId: TableRowId, update: Option[ModifiedSetUpdate], inactiveFlag: Boolean)
case class ModifiedKeyedSetNotification(tableRowId: TableRowId, update: Option[ModifiedKeyedSetUpdate], inactiveFlag: Boolean)
case class AppendSetNotification(tableRowId: TableRowId, update: Option[AppendSetUpdate], inactiveFlag: Boolean)


// Put "sequence succession" in notification?
// Don't, session is like a sub-row, we handle sub rows in parallel
// TERMINAL subscriptions that allow (re-)publishing peer to sequence w/o sessions?

case class SessId(persistenceId: UUID, instanceId: Long)

case class NotificationBatch(sets: Seq[ModifiedSetNotification], keyedSets: Seq[ModifiedKeyedSetNotification], appendSets: Seq[AppendSetNotification])

case class SessionNotificationSequence(session: SessId, batches: Seq[NotificationBatch])

case class SubscriptionNotifications(
                            localNotifications: Seq[NotificationBatch],
                            sessionNotifications: Seq[SessionNotificationSequence])

/*
case class NotificationBatch(

                            )
*/

/*
Row Transfer Model (RTM)

names:
- set replication model

PROBLEM: need data desc in-line with data updates

examples:

- manifest: prefix key? 'minimal' flag?, set modify
- [client/peer ?] endpoint set: prefix key, column-set modify
- [client/peer] index set: index key, optional value, set modify
- endpoint descs: key(s)
- data streams: set grow or modify

NOTE: endpoint set / indexes are dynamic/artificial 'tables', so prefix queries aren't really filters, they're keys to something that's generated on demand

from 'edge.manifest'
where key = ('Rankin', 'MGRID');

from 'edge.timeSeries'
where
  key = (('Rankin', 'MGRID', 'ESS'), ('OutputPower')) and
  session = 'sessA' and
  sequence = '345';


gbe.endpoint_manifest: modified_keyed_set (
  row_key: (prefix: path, simple: bool),
  element: (endpoint_id: endpoint_id, manifest_desc: bytes),
  element_key: (_.endpoint_id))

gbe.endpoint_set: modified_set (
  row_key: (prefix: path),
  element: endpoint_id)

gbe.endpoint_index_set: modified_set (
  row_key: (index: path, value: indexable_value nullable),
  data: bytes)

gbe.time_series_value: append_set (
  row_key: (endpoint_id: path, data_key: path),
  column_key: (session_id: session_id, sequence: int),
  element: bytes,
  OR
  element: (value: sample_value, time: long, quality: int nullable)
)

gbe.topic_event_value: append_set (
  row_key: (endpoint_id: path, data_key: path),
  column_key: (session_id: session_id, sequence: int),
  element: bytes,
  OR
  element: (topic: path, type: int, time: long, params: map)
)

gbe.key_value: (
  row_key: (endpoint_id: path, data_key: path),
  column_key: (session_id: session_id, sequence: int),
  element: value)

// NOTE: NEEDS TO BE DYNAMIC!!
gbe.active_keyed_set: modified_keyed_set (
  row_key: (prefix: path, simple: bool),
  element: (endpoint_id: endpoint_id, manifest_desc: bytes),
  element_key: (_.endpoint_id))

 */
