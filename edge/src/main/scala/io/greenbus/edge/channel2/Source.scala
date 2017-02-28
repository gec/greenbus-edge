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

trait Responder[A, B] {
  def handle(obj: A, respond: B => Unit)
}

trait Sender[A, B] {
  def send(obj: A, handleResponse: B => Unit): Unit
}

trait Receiver[A, B] {
  def bind(responder: Responder[A, B]): Unit
}

trait SenderChannel[A, B] extends Sender[A, B] with Closeable with CloseObservable

trait ReceiverChannel[A, B] extends Receiver[A, B] with Closeable with CloseObservable

/*
trait Receiver[A, B] {
  def bind(responder: Responder[A, B]): Unit
  def bindFunc(f: (A, Promise[B]) => Unit): Unit = {
    bind(new Responder[A, B] {
      def handle(obj: A, promise: Promise[B]): Unit = {
        f(obj, promise)
      }
    })
  }
}*/
