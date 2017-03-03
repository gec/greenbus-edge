package io.greenbus.edge.channel2

import scala.collection.mutable.ArrayBuffer

/*

object SinkOwnedSourceJoin {
  sealed trait State[A]
  case class Unbound[A](queue: ArrayBuffer[A]) extends State[A]
  case class Opened[A](handler: Handler[A]) extends State[A]
}
class SinkOwnedSourceJoin[A](bindMarshal: CallMarshaller) extends Source[A] with Sink[A] {
  import SinkOwnedSourceJoin._

  private var state: State[A] = Unbound(ArrayBuffer.empty[A])

  def bind(handler: Handler[A]): Unit = {
    bindMarshal.marshal {
      state match {
        case Unbound(queue) =>
          state = Opened(handler)
          queue.foreach(handler.handle)
        case Opened(_) =>
          // TODO: warning? exception?
          state = Opened(handler)
      }
    }
  }

  def push(obj: A): Unit = {
    state match {
      case Unbound(queue) => queue += obj
      case Opened(handler) => handler.handle(obj)
    }
  }
}
 */
object QueuedDistributor {
  sealed trait State[A]
  case class Unbound[A](queue: ArrayBuffer[A]) extends State[A]
  case class Opened[A](handler: Handler[A]) extends State[A]
}
class QueuedDistributor[A] extends Source[A] with Sink[A] {
  import QueuedDistributor._

  private var state: State[A] = Unbound(ArrayBuffer.empty[A])

  def bind(handler: Handler[A]): Unit = {
    state match {
      case Unbound(queue) =>
        state = Opened(handler)
        queue.foreach(handler.handle)
      case Opened(_) =>
        throw new IllegalStateException("Queued distributor bound twice")
    }
  }

  def push(obj: A): Unit = {
    state match {
      case Unbound(queue) => queue += obj
      case Opened(handler) => handler.handle(obj)
    }
  }
}
