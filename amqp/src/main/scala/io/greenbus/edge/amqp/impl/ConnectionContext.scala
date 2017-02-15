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
package io.greenbus.edge.amqp.impl

import org.apache.qpid.proton.engine._

trait AcceptorContext {
  def onConnectionRemoteOpen(c: Connection): Unit
}

trait ConnectionContext {
  def onOpen(c: Connection): Unit

  def onSessionRemoteOpen(s: Session): Unit

  def onRemoteClose(c: Connection): Unit
}

trait SessionContext {
  def onOpen(s: Session): Unit

  def onRemoteClose(s: Session): Unit

  def onSenderRemoteOpen(s: Sender): Unit

  def onReceiverRemoteOpen(r: Receiver): Unit
}

trait SenderContext extends LinkContext {
  def onDelivery(s: Sender, d: Delivery): Unit

  def linkFlow(s: Sender): Unit
}

trait LinkContext {
  def onOpen(l: Link): Unit

  def onRemoteClose(l: Link): Unit
}

trait ReceiverContext extends LinkContext {
  def onDelivery(r: Receiver, d: Delivery): Unit
}

trait SenderDeliveryContext {
  def onDelivery(s: Sender, delivery: Delivery): Unit
}
trait ReceiverDeliveryContext {
  def onDelivery(r: Receiver, delivery: Delivery): Unit
}