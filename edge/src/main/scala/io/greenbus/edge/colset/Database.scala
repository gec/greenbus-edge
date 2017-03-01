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

NOTE!! (later) if the thing that breaks back-propagating liveness request is parallel frontend peers to many subscribers, could get this
architecture by hooking the specific frontend peers to a leader-elected (locked) liveliness-determining master

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

trait ModifiedKeyedSetTable {
  def rowKeyType: TypeDesc
}

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

/*trait PeerPullChannel {
  def updateSubscriptionSet(modSubs: Seq[ModifiedSetSubscription], appendSubs: Seq[AppendSetSubscription])
}*/

class PeerPullProxy {


}

// TODO: subscribe direct vs. subscribe indirect (with session engine)
/*trait DatabaseView {

  //def subscribeToSetModify(table: String, rowKey: TypeValue, notify: () => Unit)
  //def subscribeToSetAppend(table: String, rowKey: TypeValue, columnQuery: TypeValue, notify: () => Unit)

  def subscribe(modSubs: Seq[ModifiedSetSubscription], appendSubs: Seq[AppendSetSubscription], notify: () => Unit): Subscription

}*/


trait Subscription {
  def dequeue(): SubscriptionNotifications
  def close(): Unit
}

case class SessionColumnQuery(sessionId: PeerSessionId, sequence: TypeValue)

case class RoutedSetSubscription(tableRowId: RoutedTableRowId, columnQuery: Option[TypeValue])
case class DirectSetSubscription(tableRowId: DirectTableRowId, columnQuery: Option[TypeValue])

//case class ModifiedSetSubscription(tableRowId: RoutedTableRowId)
//case class AppendSetSubscription(tableRowId: RoutedTableRowId, columnQuery: Option[TypeValue])
//case class AppendSetSubscription(table: String, rowKey: TypeValue, columnQuery: Option[SessionColumnQuery])

case class SubscriptionParams(directSubs: Seq[DirectSetSubscription] = Vector(), routedSubs: Seq[RoutedSetSubscription] = Vector())

// TODO: should sequence just be long, should sessions be built in? where does session-awareness go in the layering?

// TODO: IS THE WRITER TIMED OUT? can we do this within session notifications? no; how do we then handle disappearance of client publisher while peer still okay
// TODO: pull table/rowKey out, value update is seq/payload, notification is: (table/row, option(value update), option(inactive))
/*case class ModifiedSetNotification(table: String, rowKey: TypeValue, sequence: TypeValue, snapshot: Option[Set[TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class ModifiedKeyedSetNotification(table: String, rowKey: TypeValue, sequence: TypeValue, snapshot: Option[Map[TypeValue, TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class AppendSetNotification(table: String, rowKey: TypeValue, sequence: TypeValue, value: TypeValue)*/

case class RoutedTableRowId(table: SymbolVal, routingKey: TypeValue, rowKey: TypeValue)
case class ModifiedSetUpdate(sequence: TypeValue, snapshot: Option[Set[TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class ModifiedKeyedSetUpdate(sequence: TypeValue, snapshot: Option[Map[TypeValue, TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class AppendSetUpdate(sequence: TypeValue, value: TypeValue)

case class ModifiedSetNotification(tableRowId: RoutedTableRowId, update: Option[ModifiedSetUpdate], inactiveFlag: Boolean)
case class ModifiedKeyedSetNotification(tableRowId: RoutedTableRowId, update: Option[ModifiedKeyedSetUpdate], inactiveFlag: Boolean)
case class AppendSetNotification(tableRowId: RoutedTableRowId, update: Option[AppendSetUpdate], inactiveFlag: Boolean)

case class DirectTableRowId(table: SymbolVal, rowKey: TypeValue)
case class DirectModifiedSetNotification(tableRowId: DirectTableRowId, update: ModifiedSetUpdate)
case class DirectModifiedKeyedSetNotification(tableRowId: DirectTableRowId, update: ModifiedKeyedSetUpdate)
case class DirectAppendSetNotification(tableRowId: DirectTableRowId, update: AppendSetUpdate)


// Put "sequence succession" in notification?
// Don't, session is like a sub-row, we handle sub rows in parallel
// TERMINAL subscriptions that allow (re-)publishing peer to sequence w/o sessions?

case class PeerSessionId(persistenceId: UUID, instanceId: Long)

case class LocalNotificationBatch(sets: Seq[DirectModifiedSetNotification], keyedSets: Seq[DirectModifiedKeyedSetNotification], appendSets: Seq[DirectAppendSetNotification])
case class NotificationBatch(sets: Seq[ModifiedSetNotification], keyedSets: Seq[ModifiedKeyedSetNotification], appendSets: Seq[AppendSetNotification])

case class SessionNotificationSequence(session: PeerSessionId, batches: Seq[NotificationBatch])

case class SubscriptionNotifications(
                            localNotifications: Seq[LocalNotificationBatch],
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
