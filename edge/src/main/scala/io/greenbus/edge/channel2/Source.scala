package io.greenbus.edge.channel2


trait Handler[A] {
  def handle(obj: A): Unit
}
trait Source[A] {
  def bind(handler: Handler[A]): Unit
}

trait Sink[A] {
  def apply(obj: A): Unit
}

trait LatchSink {
  def apply(): Unit
}

trait LatchHandler {
  def handle(): Unit
}

trait LatchSource {
  def bind(handler: LatchHandler): Unit
}

trait Closeable {
  def close(): Unit
}

trait CloseObservable {
  def onClose()
}