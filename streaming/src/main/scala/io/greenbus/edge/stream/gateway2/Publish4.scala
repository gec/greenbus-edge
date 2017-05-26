package io.greenbus.edge.stream.gateway2

import io.greenbus.edge.flow.Sink
import io.greenbus.edge.stream.{SequenceCtx, TableRow, TypeValue}
import io.greenbus.edge.stream.gateway.RouteServiceRequest

class Publish4 {

}

sealed trait ProducerEvent
case class SetPublishEvent(key: TableRow, value: Set[TypeValue], ctxOpt: Option[SequenceCtx])

case class DynamicTableEvent()

case class RouteBindEvent(initialBatch: Seq[ProducerEvent],
                           handler: Sink[RouteServiceRequest])

/*

trait DynamicTable {
  def subscribed(key: TypeValue): Unit
  def unsubscribed(key: TypeValue): Unit
}

case class AppendPublish(key: TableRow, values: Seq[TypeValue])
case class SetPublish(key: TableRow, value: Set[TypeValue])
case class MapPublish(key: TableRow, value: Map[TypeValue, TypeValue])
case class PublishBatch(
                         appendUpdates: Seq[AppendPublish],
                         mapUpdates: Seq[MapPublish],
                         setUpdates: Seq[SetPublish])

case class RoutePublishConfig(
                               appendKeys: Seq[(TableRow, SequenceCtx)],
                               setKeys: Seq[(TableRow, SequenceCtx)],
                               mapKeys: Seq[(TableRow, SequenceCtx)],
                               dynamicTables: Seq[(String, DynamicTable)],
                               handler: Sink[RouteServiceRequest])*/
