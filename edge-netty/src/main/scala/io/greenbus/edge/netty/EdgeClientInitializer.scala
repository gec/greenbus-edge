package io.greenbus.edge.netty

import com.google.protobuf.MessageLite
import io.greenbus.edge.stream.protocol.proto.{ClientMessage, ServerMessage}
import io.netty.channel.{ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder, ProtobufVarint32FrameDecoder, ProtobufVarint32LengthFieldPrepender}

class EdgeClientInitializer(handler: SimpleChannelInboundHandler[ServerMessage]) extends ChannelInitializer[SocketChannel] {
  def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()
    p.addLast(new ProtobufVarint32FrameDecoder)
    p.addLast(new ProtobufDecoder(ServerMessage.getDefaultInstance))

    p.addLast(new ProtobufVarint32LengthFieldPrepender())
    p.addLast(new ProtobufEncoder())

    p.addLast(handler)
  }
}

class EdgeServerInitializer(handler: SimpleChannelInboundHandler[ClientMessage]) extends ChannelInitializer[SocketChannel] {
  def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()
    p.addLast(new ProtobufVarint32FrameDecoder)
    p.addLast(new ProtobufDecoder(ClientMessage.getDefaultInstance))

    p.addLast(new ProtobufVarint32LengthFieldPrepender())
    p.addLast(new ProtobufEncoder())

    p.addLast(handler)
  }
}

object EdgeProtobufInitializer {
  def client(handler: SimpleChannelInboundHandler[ServerMessage]): ChannelInitializer[SocketChannel] = {
    new EdgeProtobufInitializer[ServerMessage](ServerMessage.getDefaultInstance, handler)
  }
  def server(handler: SimpleChannelInboundHandler[ClientMessage]): ChannelInitializer[SocketChannel] = {
    new EdgeProtobufInitializer[ClientMessage](ClientMessage.getDefaultInstance, handler)
  }
}
class EdgeProtobufInitializer[A <: MessageLite](obj: A, handler: SimpleChannelInboundHandler[A]) extends ChannelInitializer[SocketChannel] {
  def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()
    p.addLast(new ProtobufVarint32FrameDecoder)
    p.addLast(new ProtobufDecoder(obj))

    p.addLast(new ProtobufVarint32LengthFieldPrepender())
    p.addLast(new ProtobufEncoder())

    p.addLast(handler)
  }
}
