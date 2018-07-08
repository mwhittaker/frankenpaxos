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
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.ByteArrayDecoder
import io.netty.handler.codec.bytes.ByteArrayEncoder
import io.netty.handler.codec.LengthFieldPrepender

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
      val localAddress = NettyTcpAddress(ctx.channel.localAddress());
      val remoteAddress = NettyTcpAddress(ctx.channel.remoteAddress());
      if (channels.contains((localAddress, remoteAddress))) {
        val err = s"A channel between remote address $remoteAddress and " +
          s"local address $localAddress is being registered, but an existing " +
          s"channel between these addresses already exists.";
        logger.error(err)
        throw new IllegalStateException(err);
      }

      println("channelRegistered " + Thread.currentThread.getId());
      channels.put((localAddress, remoteAddress), ctx.channel());
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
      msg match {
        case bytes: Array[Byte] => {
          actor.receive(remoteAddress, new String(bytes, "ASCII"))
        }
        case _ =>
          val err = "A message was received that wasn't of type Array[Byte]. " + "This is impossible.";
          logger.error(err);
          throw new IllegalStateException(err);
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

  private class CloseOnFailureFutureListener extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (!future.isSuccess()) {
        future.channel.close();
      }
    }
  }

  type Address = NettyTcpAddress
  type Timer = NettyTcpTimer

  // Map from (src, dst) addresses to channel
  // queue of timers
  private val bossEventLoop = new NioEventLoopGroup();
  private val workerEventLoop = new NioEventLoopGroup(1);

  private val actors =
    new HashMap[NettyTcpTransport#Address, Actor[NettyTcpTransport]]();
  private val channels =
    new HashMap[(NettyTcpTransport#Address, NettyTcpTransport#Address), Channel]

  def register(
      address: NettyTcpTransport#Address,
      actor: Actor[NettyTcpTransport]
  ): Unit = {
    if (actors.contains(address)) {
      logger.warn(
        s"This transport already has an actor bound to $address."
      );
      return;
    }
    actors.put(address, actor);

    new ServerBootstrap()
      .group(bossEventLoop, workerEventLoop)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel]() { // (4)
        @throws(classOf[Exception])
        override def initChannel(channel: SocketChannel): Unit = {
          logger.info(
            s"Server socket on address ${channel.localAddress} established " +
              s"connection with ${channel.remoteAddress}."
          );
          channel.pipeline.addLast(
            "frameDecoder",
            new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4)
          );
          channel.pipeline.addLast("bytesDecoder", new ByteArrayDecoder());
          channel.pipeline.addLast(new ServerHandler(actor));
          channel.pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
          channel.pipeline.addLast("bytesEncoder", new ByteArrayEncoder());
        }
      })
      .option[Integer](ChannelOption.SO_BACKLOG, 128)
      .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .bind(address.socketAddress)
      .addListeners(
        new LogFailureFutureListener(
          s"A socket could not be bound to ${address.socketAddress}."
        ),
        new CloseOnFailureFutureListener()
      );
  }

  def timer(duration: java.time.Duration): NettyTcpTransport#Timer = ???

  def send(
      src: NettyTcpTransport#Address,
      dst: NettyTcpTransport#Address,
      msg: String
  ): Unit = {
    val actor = actors.get(src) match {
      case Some(actor) => actor
      case None =>
        logger.warn(
          s"Attempted to send from $src to $dst, but no actor for address " +
            s"$src was found."
        );
        return;
    }

    channels.get((src, dst)) match {
      case Some(channel) => {
        channel
          .writeAndFlush(msg.getBytes())
          .addListener(
            new LogFailureFutureListener(
              s"Unable to send message from $src to $dst."
            )
          )
      }
      case None => {
        new Bootstrap()
          .group(bossEventLoop)
          .channel(classOf[NioSocketChannel])
          .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
          .handler(new ChannelInitializer[SocketChannel]() {
            @throws(classOf[Exception])
            override def initChannel(channel: SocketChannel) {
              channel.pipeline.addLast(
                "frameDecoder",
                new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4)
              );
              channel.pipeline
                .addLast("bytesDecoder", new ByteArrayDecoder());
              channel.pipeline.addLast(new ServerHandler(actor));
              channel.pipeline
                .addLast("frameEncoder", new LengthFieldPrepender(4));
              channel.pipeline
                .addLast("bytesEncoder", new ByteArrayEncoder());
            }
          })
          .connect(dst.socketAddress)
          .addListeners(
            new LogFailureFutureListener(s"Unable to connect to $dst."),
            new CloseOnFailureFutureListener(),
            new ChannelFutureListener() {
              override def operationComplete(future: ChannelFuture): Unit = {
                if (future.isSuccess()) {
                  logger.info(
                    s"Client socket on address " +
                      s"${future.channel.localAddress} established " +
                      s"connection with ${future.channel.remoteAddress}."
                  );

                  // TODO(mwhittaker): Check that there's not already an actor
                  // bound to this address.
                  actors
                    .put(NettyTcpAddress(future.channel.localAddress), actor);

                  future.channel
                    .writeAndFlush(msg.getBytes())
                    .addListener(
                      new LogFailureFutureListener(
                        s"Unable to send message from $src to $dst."
                      )
                    );
                }
              }
            }
          );
      }
    }
  }
}
