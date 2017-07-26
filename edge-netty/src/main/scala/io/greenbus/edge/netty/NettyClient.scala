package io.greenbus.edge.netty

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.ReferenceCountUtil

object NettyClient {

  def main(args: Array[String]): Unit = {
    val workerGroup = new NioEventLoopGroup()

    try {
      val b = new Bootstrap()
      b.group(workerGroup)
      b.channel(classOf[NioSocketChannel])
      b.option(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)
      b.handler(new ChannelInitializer[SocketChannel]() {
        def initChannel(c: SocketChannel): Unit = {
          c.pipeline().addLast(new MyClientHandler)
        }
      })

      val f = b.connect("127.0.0.1", 7777).sync()
      f.channel().closeFuture().sync()
    } finally {
      workerGroup.shutdownGracefully()
    }
  }
}

class MyClientHandler extends ChannelInboundHandlerAdapter {

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    msg match {
      case bb: ByteBuf =>
        try {
          println(bb)
          ctx.close()
        } finally {
          ReferenceCountUtil.release(msg)
        }
      case _ =>
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}
