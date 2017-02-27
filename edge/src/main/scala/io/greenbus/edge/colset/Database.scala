package io.greenbus.edge.colset


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

trait SubscriberProxy {

}



trait Database {

  def create()

  // for active sets
  def createDynamic()

  def transaction()

}

// TODO: subscribe direct vs. subscribe indirect (with session engine)
trait DatabaseView {

  //def subscribeToSetModify(table: String, rowKey: TypeValue, notify: () => Unit)
  //def subscribeToSetAppend(table: String, rowKey: TypeValue, columnQuery: TypeValue, notify: () => Unit)

  def subscribe(modSubs: Seq[ModifiedSetSubscription], appendSubs: Seq[AppendSetSubscription], notify: () => Unit): Subscription

}

trait Subscription {
  def dequeue(): NotificationBatch
  def close(): Unit
}

case class ModifiedSetSubscription(table: String, rowKey: TypeValue)
case class AppendSetSubscription(table: String, rowKey: TypeValue, columnQuery: TypeValue)

// TODO: should sequence just be long, should sessions be built in? where does session-awareness go in the layering?
case class ModifiedSetNotification(table: String, rowKey: TypeValue, sequence: TypeValue, snapshot: Option[Set[TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class ModifiedKeyedSetNotification(table: String, rowKey: TypeValue, sequence: TypeValue, snapshot: Option[Map[TypeValue, TypeValue]], removes: Set[TypeValue], adds: Set[TypeValue])
case class AppendSetNotification(table: String, rowKey: TypeValue, sequence: TypeValue)

// Put "sequence succession" in notification?
// Don't, session is like a sub-row, we handle sub rows in parallel


//case class NotificationSequence()
case class NotificationBatch()

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
