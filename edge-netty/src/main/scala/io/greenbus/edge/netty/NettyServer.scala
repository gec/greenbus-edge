package io.greenbus.edge.netty

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel._
import io.netty.util.ReferenceCountUtil

object NettyServer {

  def main(args: Array[String]): Unit = {
    val bossGroup = new NioEventLoopGroup()
    val workerGroup = new NioEventLoopGroup()
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new ChannelInitializer[SocketChannel]() {
          def initChannel(c: SocketChannel): Unit = {
            c.pipeline().addLast(new MyServerHandler)
          }
        })
        .option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 128)
        .childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)

      val f = b.bind(7777).sync()
      f.channel().closeFuture().sync()
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}


class MyServerHandler extends ChannelInboundHandlerAdapter {


  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val time = ctx.alloc().buffer(4)
    time.writeInt(1234)
    val f = ctx.writeAndFlush(time)
    f.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture): Unit = {
        ctx.close()
      }
    })

  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    /*msg match {
      case bb: ByteBuf => bb.release()
      case _ =>
    }*/
    try {
      println(msg)
      ctx.write(msg)
      ctx.flush()
    } finally {
      ReferenceCountUtil.release(msg)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}