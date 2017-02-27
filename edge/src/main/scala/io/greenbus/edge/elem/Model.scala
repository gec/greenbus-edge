package io.greenbus.edge.elem


/*

THEME: simple things that just need to be organized properly

user -> [pub client ->] peer sub replica -> peer merged pub replica -> [(peer) ->] sub client -> user

problems:
sessions / sequences / merging
storage (and how to query)
backpressure / compaction / QoS
sub index map for key ids for bandwidth
symbols vs. identifiers vs. strings vs. text
endpoint UUID registration
partitions, namespaces (local -> public? always a mapping between )
communicate avail? (or unavail, more like)
ts quality
loss of sync in-line in data subscription?
protocol extensibility


can ouputs be re-modeled as non-singular publishes?

Transmit indexes in peer manifest, not data keys?

ideas:
cql-like schema model, endpoints are "row sets"
well-understood GB edge types layered on top of this?

two models, column-set grow / column-set update

time series and events vary by: partitionability (sub-sets?), data schema, reducibility

user API support is a separate problem layered on top of the fundamental model

can everything below the schema be pushed to a new amqp layer

are endpoint sets and index sets just a different type of "table" as far as the pub/sub is concerned
  - do the replication peers communicate the "table" sets it has?
  - are endpoint descriptors???
  - are manifests??????

layers:
row transfer model ->
[data space (partition) model??] ->
endpoint / data / output model ->
data type schemas (ts / events / kvs) ->
metamodels (labeled-integer-status, interpolated-series) | user semantics (solar output power)

compare contrast DDS:
transfer model -> data model -> user model


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











????
from 'edge.timeSeries'
where key = (('Rankin', 'MGRID', 'ESS'), ('OutputPower')) and
      session = 'sessA' and
      sequence = '345'
sectionBy (
      from 'edge.dataDescs' where key = (('Rankin', 'MGRID', 'ESS'), ('OutputPower'))
      );


CREATE SETMOD TABLE edge.manifest (
  prefix path PRIMARY_KEY,
  endpoint_id path SET_KEY,
  manifest_desc bytes
)

CREATE SETAPPEND TABLE edge.timeSeries (
  endpoint_id path,
  data_key path,
  session_id (uuid, uint64),
  sequence uint64,
  PRIMARY_KEY (endpoint_id, data_key),
  SET_ID (session_id, sequence)
)


 */

class Model {

}
