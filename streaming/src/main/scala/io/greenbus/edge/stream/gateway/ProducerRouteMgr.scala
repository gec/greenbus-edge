/**
 * Copyright 2011-2017 Green Energy Corp.
 *
 * Licensed to Green Energy Corp (www.greenenergycorp.com) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. Green Energy
 * Corp licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.greenbus.edge.stream.gateway

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow.Sink
import io.greenbus.edge.stream.engine.{ KeyStreamObserver, RouteTargetSubject, StreamObserver }
import io.greenbus.edge.stream.{ PeerSessionId, TableRow }

import scala.collection.mutable

class ProducerRouteMgr(appendLimitDefault: Int) extends RouteTargetSubject with LazyLogging {

  private var produced = false
  private val updateMap = mutable.Map.empty[TableRow, ProducerUpdateStream]
  private var dynamicTableMap = Map.empty[String, DynamicTable]
  private var requestHandlerOpt = Option.empty[Sink[RouteServiceRequest]]

  private val subjectMap = mutable.Map.empty[TableRow, ProducerStreamSubject]
  private val subscriptionMap = mutable.Map.empty[StreamObserver, Map[TableRow, KeyStreamObserver]]

  def isProduced: Boolean = produced

  def bind(events: Seq[ProducerKeyEvent], dynamic: Map[String, DynamicTable], handler: Sink[RouteServiceRequest]): Unit = {
    logger.debug(s"Subject map before bind: " + subjectMap.keySet)
    logger.debug(s"Subscription map before bind: " + subscriptionMap)
    unbind()

    subscriptionMap.foreach {
      case (obs, subscriptionSet) =>
        subscriptionSet.foreach {
          case (key, obs) =>
            observerAdded(key, obs)
        }
    }

    bindTables(dynamic)
    events.foreach(handleEvent)
    requestHandlerOpt = Some(handler)
    produced = true
  }

  def batch(events: Seq[ProducerKeyEvent]): Unit = {
    events.foreach(handleEvent)
  }
  def request(request: RouteServiceRequest): Unit = {
    requestHandlerOpt.foreach(_.push(request))
  }

  def unbind(): Unit = {
    logger.debug(s"Route unbound")
    updateMap.clear()
    dynamicTableMap = Map()
    //subjectMap.foreach { case (row, subj) => subj.unbind() }
    subjectMap.clear()
    produced = false
  }

  private def bindTables(dynamic: Map[String, DynamicTable]): Unit = {
    dynamicTableMap = dynamic

    val tableSubjects: Map[String, collection.Map[TableRow, ProducerStreamSubject]] =
      subjectMap.filterKeys(row => dynamicTableMap.keySet.contains(row.table))
        .groupBy(_._1.table)

    tableSubjects.foreach {
      case (table, rowMap) =>
        dynamic.get(table).foreach { t =>
          rowMap.keys.foreach(row => t.subscribed(row.rowKey))
        }
    }
  }

  private def handleEvent(ev: ProducerKeyEvent): Unit = {
    ev match {
      case AddRow(key, ctx) => {
        //updateMap.get(key).foreach { stream => } // TODO: remove row?
        val stream = new ProducerUpdateStream(PeerSessionId(UUID.randomUUID(), 0), ctx, appendLimitDefault)
        streamAdded(key, stream)
      }
      case RowUpdate(key, update) => {
        //updateMap.get(key).foreach(_.handle(update))

        updateMap.get(key) match {
          case None => logger.debug(s"No stream for key: " + key)
          case Some(handler) =>
            handler.handle(update)
        }

      }
      case DropRow(key) =>
        updateMap.get(key).foreach(s => streamRemoved(key, s))
    }
  }

  private def streamAdded(key: TableRow, stream: ProducerUpdateStream): Unit = {
    logger.debug(s"Stream added $key; subjects: " + subjectMap.get(key))
    updateMap.update(key, stream)
    subjectMap.get(key).foreach { subj =>
      subj.bind(stream.cache)
      stream.bind(subj)
    }
  }
  private def streamRemoved(key: TableRow, stream: ProducerUpdateStream): Unit = {
    logger.debug(s"Stream removed $key")
    updateMap -= key
    subjectMap.get(key).foreach(subj => subj.unbind())
  }

  private def observerAdded(key: TableRow, observer: KeyStreamObserver): Unit = {
    logger.debug(s"observer added $key")
    subjectMap.get(key) match {
      case None =>
        dynamicTableMap.get(key.table) match {
          case None => staticSubjectAdded(key, observer)
          case Some(table) => dynamicSubjectAdded(key, table, observer)
        }
      case Some(subject) =>
        subject.targetAdded(observer)
    }
  }
  private def observerRemoved(key: TableRow, observer: KeyStreamObserver): Unit = {
    logger.debug(s"observer removed $key")
    subjectMap.get(key).foreach { subject =>
      subject.targetRemoved(observer)
      if (!subject.targeted()) {
        dynamicTableMap.get(key.table) match {
          case None => staticSubjectRemoved(key, subject)
          case Some(table) => dynamicSubjectRemoved(key, table, subject)
        }
      }
    }
  }

  private def buildAndBindSubject(key: TableRow, static: Boolean): ProducerStreamSubject = {
    val subject = new ProducerStreamSubject(absentWhenUnbound = static)
    subjectMap.update(key, subject)
    updateMap.get(key).foreach { producer =>
      subject.bind(producer.cache)
      producer.bind(subject)
    }
    subject
  }

  private def staticSubjectAdded(key: TableRow, observer: KeyStreamObserver): Unit = {
    logger.debug(s"observer added $key")
    val subject = buildAndBindSubject(key, static = true)
    subject.targetAdded(observer)
  }
  private def dynamicSubjectAdded(key: TableRow, table: DynamicTable, observer: KeyStreamObserver): Unit = {
    logger.debug(s"observer added $key")
    val subject = buildAndBindSubject(key, static = false)
    subject.targetAdded(observer)
    table.subscribed(key.rowKey)
  }
  private def staticSubjectRemoved(key: TableRow, subject: ProducerStreamSubject): Unit = {
    updateMap.get(key).foreach { stream => stream.unbind() }
    subjectMap -= key
  }
  private def dynamicSubjectRemoved(key: TableRow, table: DynamicTable, subject: ProducerStreamSubject): Unit = {
    updateMap.get(key).foreach { stream => stream.unbind() }
    subjectMap -= key
    table.unsubscribed(key.rowKey)
  }

  def targeted(): Boolean = {
    subjectMap.keySet.nonEmpty || produced
    //subscriptionMap.nonEmpty || produced
  }

  def targetUpdate(target: StreamObserver, subscription: Map[TableRow, KeyStreamObserver]): Unit = {
    logger.debug(s"targetUpdate: $subscription")
    val previous = subscriptionMap.getOrElse(target, Map())
    val removes = previous.keySet -- subscription.keySet

    removes.flatMap(row => previous.get(row).map(obs => (row, obs))).foreach {
      case (key, obs) =>
        observerRemoved(key, obs)
    }

    subscription.foreach {
      case (key, obs) =>
        observerAdded(key, obs)
    }

    val untargetedStreams = removes.filter(key => subjectMap.get(key).exists(!_.targeted()))
    logger.debug(s"Removing untargeted: " + untargetedStreams)
    subjectMap --= untargetedStreams

    subscriptionMap.update(target, subscription)
  }

  def targetRemoved(target: StreamObserver): Unit = {
    val previous = subscriptionMap.getOrElse(target, Map())

    logger.debug(s"targetRemoved: ${previous.keySet}")

    previous.foreach {
      case (key, obs) =>
        observerRemoved(key, obs)
    }

    subscriptionMap -= target
  }
}
