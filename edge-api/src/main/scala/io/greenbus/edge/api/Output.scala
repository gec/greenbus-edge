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
package io.greenbus.edge.api

import java.util.UUID

case class SessionId(persistenceId: UUID, instanceId: Long)

sealed trait OutputResult
case class OutputSuccess(valueOpt: Option[Value]) extends OutputResult
case class OutputFailure(reason: String) extends OutputResult

case class OutputKeyStatus(sessionId: SessionId, sequence: Long, valueOpt: Option[Value])

case class OutputRequest(key: Path, value: OutputParams, correlation: Long)

case class OutputParams(sessionOpt: Option[SessionId] = None,
  sequenceOpt: Option[Long] = None,
  compareValueOpt: Option[Value] = None,
  outputValueOpt: Option[Value] = None)