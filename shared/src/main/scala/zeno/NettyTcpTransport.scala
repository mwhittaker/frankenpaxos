package zeno

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import java.net.SocketAddress;
import scala.collection.mutable.HashMap;
import scala.util.Try;
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.channel.ChannelOption
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.ByteBuf
import java.util.concurrent.TimeUnit
import io.netty.channel.Channel
import io.netty.bootstrap.Bootstrap
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelFuture

case class NettyTcpAddress(socketAddress: SocketAddress) extends zeno.Address

class NettyTcpTimer extends zeno.Timer {
  def name(): String = ???
  def start(): Unit = ???
  def stop(): Unit = ???
  def reset(): Unit = ???
}

class NettyTcpTransport(private val logger: Logger)
    extends Transport[NettyTcpTransport] {
  private class ServerHandler(private val actor: Actor[NettyTcpTransport])
      extends ChannelInboundHandlerAdapter {
    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      println("channelActive " + Thread.currentThread().getId());
      ctx.fireChannelActive();
    }

    override def channelInactive(ctx: ChannelHandlerContext): Unit = {
      println("channelInactive " + Thread.currentThread.getId());
      ctx.fireChannelInactive();
    }

    override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
      println("channelReadComplete " + Thread.currentThread.getId());
      ctx.fireChannelReadComplete();
    }

    override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
      println("channelRegistered " + Thread.currentThread.getId());
      ctx.fireChannelRegistered();
    }

    override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
      println("channelUnregistered " + Thread.currentThread.getId());
      ctx.fireChannelUnregistered();
    }

    override def exceptionCaught(
        ctx: ChannelHandlerContext,
        exn: java.lang.Throwable
    ): Unit = {
      println("exceptionCaught " + Thread.currentThread.getId());
      ctx.fireExceptionCaught(exn);
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = { // (2)
      val localAddress = NettyTcpAddress(ctx.channel.localAddress)
      val remoteAddress = NettyTcpAddress(ctx.channel.remoteAddress)
      if (!channels.contains((localAddress, remoteAddress))) {
        channels.put(
          (localAddress, remoteAddress),
          ctx.channel
        )
      }

      msg match {
        case buf: ByteBuf => {
          actor.receive(
            remoteAddress,
            buf.toString(io.netty.util.CharsetUtil.US_ASCII)
          )
        }
        case _ =>
          // TODO(mwhittaker): Handle more gracefully.
          ???
      }
    }
  }

  private class LogFailureFutureListener(message: String)
      extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (future.isSuccess()) {
        return;
      }

      if (future.isCancelled()) {
        logger.warn(s"Future was cancelled: $message");
      } else {
        logger.warn(s"Future failed: $message");
        logger.warn(future.cause().getStackTraceString);
      }
    }
  }

  type Address = NettyTcpAddress
  type Timer = NettyTcpTimer

  // Map from (src, dst) addresses to channel
  // queue of timers
  private val bossEventLoop = new NioEventLoopGroup();
  private val workerEventLoop = new NioEventLoopGroup(1);
  private val serverBootstrap = new ServerBootstrap();
  private val clientBootstrap = new Bootstrap();

  private val actors =
    new HashMap[NettyTcpTransport#Address, Actor[NettyTcpTransport]]();
  private val channels =
    new HashMap[(NettyTcpTransport#Address, NettyTcpTransport#Address), Channel]

  // Constructor
  serverBootstrap
    .group(bossEventLoop, workerEventLoop)
    .channel(classOf[NioServerSocketChannel])
    .childHandler(new ChannelInitializer[SocketChannel]() { // (4)
      @throws(classOf[Exception])
      override def initChannel(ch: SocketChannel): Unit = {
        actors.get(NettyTcpAddress(ch.localAddress())) match {
          case Some(actor) => ch.pipeline().addLast(new ServerHandler(actor));
          case None        =>
            // TODO(mwhittaker): Log a warning instead.
            println(
              s"A socket bound to ${ch.localAddress()} was " +
                s"initialized, but there is no actor for this address."
            )
        }
      }
    })
    .option[Integer](ChannelOption.SO_BACKLOG, 128)
    .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true);

  clientBootstrap
    .group(bossEventLoop)
    .channel(classOf[NioSocketChannel])
    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    .handler(new ChannelInitializer[SocketChannel]() {
      @throws(classOf[Exception])
      override def initChannel(ch: SocketChannel) {
        // ch.pipeline().addLast(new TimeClientHandler());
      }
    });

  def register(
      address: NettyTcpTransport#Address,
      actor: Actor[NettyTcpTransport]
  ): Try[Unit] = {
    // Store the actor in actors.
    if (actors.contains(address)) {
      return Try(
        throw new IllegalStateException(
          s"This transport already has an actor bound to address $address."
        )
      )
    }
    assert(actors.put(address, actor).isEmpty);

    // Launch the server.
    serverBootstrap
      .bind(address.socketAddress)
      .addListener(
        new LogFailureFutureListener(
          s"A socket could not be bound to ${address.socketAddress}."
        )
      );

    Try(())
  }

  def timer(duration: java.time.Duration): NettyTcpTransport#Timer = ???

  def send(
      src: NettyTcpTransport#Address,
      dst: NettyTcpTransport#Address,
      msg: String
  ): Unit = {
    val channel = channels.get((src, dst)) match {
      case Some(channel) => channel
      case None =>
        val channel =
          clientBootstrap.connect(dst.socketAddress).sync().channel();
        channels.put((src, dst), channel);
        channel
    }

    val buf = Unpooled.buffer(1000);
    buf.writeBytes(msg.getBytes());
    channel.writeAndFlush(buf).sync();
  }

  def run(): Unit = {
    // bossEventLoop.terminationFuture().sync()
    // assert(bossEventLoop.awaitTermination(0, TimeUnit.SECONDS));
  }
}
