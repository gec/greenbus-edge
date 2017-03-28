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
package io.greenbus.edge.peer

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow.CloseableComponent
import io.greenbus.edge.thread.SchedulableCallMarshaller

import scala.concurrent.{ ExecutionContext, Future }

object Retrier {
  sealed trait State[+A <: CloseableComponent]
  case class RetryPending(sequence: Long) extends State[Nothing]
  case class AttemptPending(sequence: Long, attemptTime: Long) extends State[Nothing]
  case class Connected[A <: CloseableComponent](sequence: Long, result: A, successTime: Long) extends State[A]
  case object Closed extends State[Nothing]
}
class Retrier[A <: CloseableComponent](logId: String,
    eventThread: SchedulableCallMarshaller,
    attempt: () => Future[A],
    handle: A => Unit,
    retryIntervalMs: Long)(implicit ec: ExecutionContext) extends LazyLogging {
  import Retrier._

  private var state: State[A] = RetryPending(0)

  def start(): Unit = {
    eventThread.marshal(attempt(0))
  }

  def close(): Unit = {
    eventThread.marshal {
      val prevState = state
      state = Closed
      prevState match {
        case Connected(_, result, _) => result.close()
        case st =>
      }
    }
  }

  private def attempt(sequence: Long): Unit = {
    state match {
      case st: RetryPending => {
        if (sequence == st.sequence) {

          logger.info(s"$logId attempting connection")
          val fut = attempt()

          fut.foreach { result => eventThread.marshal(onAttemptSuccess(sequence, result)) }
          fut.failed.foreach(ex => eventThread.marshal(onAttemptFailure(sequence, ex)))

          state = AttemptPending(sequence, System.currentTimeMillis())
        }
      }
      case Closed => logger.debug(s"$logId not attempting because closed")
      case st => {
        logger.debug(s"$logId saw attempt in wrong state: $st")
      }
    }
  }

  private def onAttemptSuccess(sequence: Long, result: A): Unit = {
    state match {
      case st: AttemptPending => {
        if (st.sequence == sequence) {
          logger.info(s"$logId attempt success")
          val nextSeq = st.sequence + 1
          state = Connected(nextSeq, result, System.currentTimeMillis())
          result.onClose.subscribe(() => eventThread.marshal(onResultClose(nextSeq)))
          handle(result)
        } else {
          logger.info(s"$logId delayed attempt success")
          result.close()
        }
      }
      case RetryPending(_) => {
        logger.debug(s"$logId got connected event while retry pending")
        result.close()
      }
      case Connected(_, _, _) => {
        logger.debug(s"$logId got connected event while already connected")
        result.close()
      }
      case Closed => {
        result.close()
      }
    }
  }

  private def onAttemptFailure(sequence: Long, ex: Throwable): Unit = {
    state match {
      case st: AttemptPending => {
        if (st.sequence == sequence) {
          logger.info(s"$logId connection failure: " + ex)

          val nextSeq = st.sequence + 1
          state = RetryPending(nextSeq)
          issueOrScheduleRetry(nextSeq, st.attemptTime)

        } else {
          logger.info(s"$logId delayed connection failure: " + ex)
        }
      }
      case st: RetryPending => {
        logger.info(s"$logId connection failure while retry pending: " + ex)
      }
      case Connected(_, _, _) => {
        logger.info(s"$logId connection failure while connected: " + ex)
      }
      case Closed => {
        logger.info(s"$logId connection failure while closed: " + ex)
      }
    }
  }

  private def issueOrScheduleRetry(seq: Long, lastTime: Long): Unit = {
    val now = System.currentTimeMillis()
    val elapsed = now - lastTime
    if (elapsed > retryIntervalMs) {
      attempt(seq)
    } else {
      eventThread.delayed(retryIntervalMs - elapsed, { attempt(seq) })
    }
  }

  private def onResultClose(sequence: Long): Unit = {
    logger.debug(s"Result close: $sequence")
    state match {
      case Connected(currSeq, _, successTime) => {
        if (currSeq == sequence) {

          val nextSeq = currSeq + 1
          state = RetryPending(nextSeq)
          issueOrScheduleRetry(nextSeq, successTime)

        } else {
          logger.info(s"$logId delayed connection close")
        }
      }
      case st => {
        logger.debug(s"$logId ignoring closed in state $st")
      }
    }
  }
}