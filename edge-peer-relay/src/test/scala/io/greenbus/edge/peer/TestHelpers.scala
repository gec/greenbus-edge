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

import io.greenbus.edge.api.IdentifiedEdgeUpdate
import org.scalatest.exceptions.TestFailedException

import scala.annotation.tailrec
import scala.concurrent.{ Await, Future, Promise, promise }

object TestHelpers {

  class TypedEventQueue[A] {

    private var queue = Seq.empty[A]
    private var chk = Option.empty[(Promise[Seq[A]], Seq[A] => Boolean)]

    private val mutex = new Object

    def received(obj: A): Unit = {
      mutex.synchronized {
        queue = queue ++ Vector(obj)
        chk.foreach {
          case (prom, check) =>
            if (check(queue)) {
              val q = queue
              queue = Seq.empty[A]
              prom.success(q)
              chk = None
            }
        }
      }
    }

    def listen(check: Seq[A] => Boolean): Future[Seq[A]] = {
      mutex.synchronized {
        val prom = Promise[Seq[A]]

        if (check(queue)) {
          val q = queue
          queue = Seq.empty[A]
          prom.success(q)
        } else {
          chk = Some((prom, check))
        }
        prom.future
      }
    }
  }

  type CheckAndPartition[A] = Seq[A] => Option[(Seq[A], Seq[A])]

  class FlattenedEventQueue[A] {
    private val mutex = new Object
    private var queue = Vector.empty[A]
    private var chk = Option.empty[(Promise[Seq[A]], CheckAndPartition[A])]

    def received(obj: Seq[A]): Unit = {
      mutex.synchronized {
        queue = queue ++ obj
        chk.foreach {
          case (prom, check) =>

            try {
              check(queue) match {
                case None =>
                  chk = Some(prom, check)
                case Some((consumed, remains)) => {
                  queue = remains.toVector
                  prom.success(consumed)
                  chk = None
                }
              }
            } catch {
              case ex: Throwable =>
                if (!prom.isCompleted) {
                  prom.failure(ex)
                }
            }
        }
      }
    }

    def listen(check: CheckAndPartition[A]): Future[Seq[A]] = {
      mutex.synchronized {
        val prom = Promise[Seq[A]]
        try {
          check(queue) match {
            case None =>
              chk = Some(prom, check)
            case Some((consumed, remains)) => {
              queue = remains.toVector
              prom.success(consumed)
            }
          }
        } catch {
          case ex: Throwable => prom.failure(ex)
        }

        prom.future
      }
    }

    import scala.concurrent.duration._

    def awaitListen(check: CheckAndPartition[A], durationMs: Long): Unit = {
      val fut = listen(check)

      try {
        Await.result(fut, durationMs.milliseconds)
      } catch {
        case ex: Throwable =>
          val q = mutex.synchronized { queue }
          throw new TestFailedException(s"Queue never matched: $q", ex, 10)
      }
    }
  }

  sealed trait SeqMatcher[A]
  case class FixedMatcher[A](f: PartialFunction[A, Boolean]) extends SeqMatcher[A]
  case class EventualMatcher[A](f: PartialFunction[A, Boolean]) extends SeqMatcher[A]

  def prefixMatcher[A](matchers: Seq[SeqMatcher[A]]): CheckAndPartition[A] = {

    @tailrec
    def run(objs: Seq[A], matchers: Seq[SeqMatcher[A]], consumed: Seq[A]): Option[(Seq[A], Seq[A])] = {
      (objs.headOption, matchers.headOption) match {
        case (Some(obj), Some(m)) => {
          m match {
            case FixedMatcher(fun) =>
              if (fun.isDefinedAt(obj) && fun(obj)) {
                run(objs.tail, matchers.tail, consumed :+ obj)
              } else {
                // TODO: will never work
                throw new IllegalStateException(s"Fixed matcher did not match")
              }
            case EventualMatcher(fun) =>
              if (fun.isDefinedAt(obj) && fun(obj)) {
                run(objs.tail, matchers.tail, consumed :+ obj)
              } else {
                run(objs.tail, matchers, consumed :+ obj)
              }
          }
        }
        case (Some(_), None) => Some((consumed, objs))
        case (None, Some(_)) => None
        case (None, None) => Some((consumed, Seq()))
      }
    }

    run(_, matchers, Seq())
  }

  //def testWait()
}

trait TypedSubHelpers[SubType] {
  import TestHelpers._

  type FlatQueue = FlattenedEventQueue[SubType]

  def prefixMatcher(matchers: SeqMatcher[SubType]*): CheckAndPartition[SubType] = {
    TestHelpers.prefixMatcher(matchers)
  }

  def fixed(f: PartialFunction[SubType, Boolean]): SeqMatcher[SubType] = {
    TestHelpers.FixedMatcher[SubType](f)
  }
  def eventual(f: PartialFunction[SubType, Boolean]): SeqMatcher[SubType] = {
    TestHelpers.EventualMatcher[SubType](f)
  }
}
