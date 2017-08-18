package io.greenbus.edge.netty

import io.greenbus.edge.stream.{RowId, TupleVal}

import scala.collection.mutable

/*

alias add
alias remove

set (prefix) subscriptions
set events

event subscription set
event batches

blob requests
blob responses
service requests
service responses

 */
class ChannelMachine {

  private val aliases = mutable.Map.empty[Long, RowId]

  def aliasesAdded(ids: Seq[(Long, RowId)]): Unit = {
    ids.foreach { case (alias, row) => aliases.put(alias, row) }
  }
  def aliasesRemoved(ids: Seq[Long]): Unit = {
    ids.foreach(aliases.remove)
  }

}
