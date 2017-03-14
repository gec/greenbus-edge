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
package io.greenbus.edge.channel2

import io.greenbus.edge.flow.{ ReceiverChannel, SenderChannel }

/*trait ChannelDescriptor[Message] {
  type Sender = SenderChannel[Message, Boolean]
  type Receiver = ReceiverChannel[Message, Boolean]
}*/

trait ChannelGroupKey

trait ChannelDescriptor[Message] {
  //def groupKey: ChannelGroupKey
  type Sender = SenderChannel[Message, Boolean]
  type Receiver = ReceiverChannel[Message, Boolean]
}

trait ChannelSerializationProvider {
  def serializerFor[A](desc: ChannelDescriptor[A]): A => Array[Byte]
  def deserializerFor[A](desc: ChannelDescriptor[A]): (Array[Byte], Int) => Either[String, A]
}

trait ChannelServerHandler {
  def handleReceiver[Message](desc: ChannelDescriptor[Message], channel: ReceiverChannel[Message, Boolean]): Unit
  def handleSender[Message](desc: ChannelDescriptor[Message], channel: SenderChannel[Message, Boolean]): Unit
}
