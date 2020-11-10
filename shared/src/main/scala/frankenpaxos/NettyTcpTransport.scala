package frankenpaxos

import collection.mutable
import com.google.protobuf.ByteString
import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelOutboundInvoker
import io.netty.channel.ChannelPromise
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.codec.bytes.ByteArrayDecoder
import io.netty.handler.codec.bytes.ByteArrayEncoder
import io.netty.handler.logging.LoggingHandler
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.ScheduledFuture
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.util.Try

case class NettyTcpAddress(socketAddress: SocketAddress)
    extends frankenpaxos.Address

object HostPortSerializer extends ProtoSerializer[HostPort]

object NettyTcpAddressSerializer
    extends frankenpaxos.Serializer[NettyTcpAddress] {
  // A cache of addresses, used to speed up fromByteString.
  private val fromByteStringCache = mutable.Map[ByteString, NettyTcpAddress]()

  override def toBytes(x: NettyTcpAddress): Array[Byte] = {
    // We get SocketAddresses from Netty. The Netty documentation informs you
    // to downcast the SocketAddress to the appropriate concrete type (e.g.,
    // InetSocketAddress). That's what we do here.
    val address = x.socketAddress.asInstanceOf[InetSocketAddress]
    val hostport = new HostPort(address.getHostString(), address.getPort())
    HostPortSerializer.toBytes(hostport)
  }

  override def fromBytes(bytes: Array[Byte]): NettyTcpAddress = {
    val hostport = HostPortSerializer.fromBytes(bytes)
    NettyTcpAddress(new InetSocketAddress(hostport.host, hostport.port))
  }

  override def fromByteString(bytes: ByteString): NettyTcpAddress = {
    fromByteStringCache.get(bytes) match {
      case Some(address) => address
      case None =>
        val address = fromBytes(bytes.toByteArray)
        fromByteStringCache(bytes) = address
        address
    }
  }

  override def toPrettyString(x: NettyTcpAddress): String =
    x.socketAddress.toString()
}

// TODO(mwhittaker): Make constructor private.
class NettyTcpTimer(
    eventLoop: EventLoopGroup,
    logger: Logger,
    name: String,
    delay: java.time.Duration,
    f: () => Unit
) extends frankenpaxos.Timer {
  private var scheduledFuture: Option[ScheduledFuture[Unit]] = None

  override def name(): String = {
    name
  }

  override def start(): Unit = {
    scheduledFuture match {
      case Some(_) =>
      // The timer is already started, so we don't have to do anything.

      case None =>
        val callable = new Callable[Unit]() {
          override def call(): Unit = {
            scheduledFuture = None
            f()
          }
        }
        scheduledFuture = Some(
          eventLoop.schedule(
            callable,
            delay.toNanos(),
            java.util.concurrent.TimeUnit.NANOSECONDS
          )
        )
    }
  }

  override def stop(): Unit = {
    scheduledFuture match {
      case Some(future) =>
        future.cancel(false)
        scheduledFuture = None
      case None =>
      // The timer is already stopped, so we don't have to do anything.
    }
  }
}

class NettyTcpTransport(private val logger: Logger)
    extends Transport[NettyTcpTransport] {

  private class CommonHandler(protected val actor: Actor[NettyTcpTransport])
      extends ChannelInboundHandlerAdapter {
    override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
      val localAddress = NettyTcpAddress(ctx.channel.localAddress)
      val remoteAddress = NettyTcpAddress(ctx.channel.remoteAddress)
      msg match {
        case bytes: Array[Byte] => {
          actor.receive(remoteAddress, actor.serializer.fromBytes(bytes))
        }
        case _ =>
          logger.fatal(
            "A message was received that wasn't of type Array[Byte]. " +
              "This is impossible."
          )
      }
    }

    override def exceptionCaught(
        ctx: ChannelHandlerContext,
        cause: Throwable
    ): Unit = {
      val localAddress = NettyTcpAddress(ctx.channel.localAddress)
      val remoteAddress = NettyTcpAddress(ctx.channel.remoteAddress)

      val stringWriter = new java.io.StringWriter()
      cause.printStackTrace(new java.io.PrintWriter(stringWriter));

      logger.warn(
        s"An exception was caught from $remoteAddress to $localAddress.\n" +
          s"$cause\n" +
          s"$stringWriter"
      )

      // TODO(mwhittaker): Close the channel? I'm not sure how exceptions and
      // unregistering channels relate.
    }
  }

  private class ServerHandler(actor: Actor[NettyTcpTransport])
      extends CommonHandler(actor) {
    override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
      val localAddress = NettyTcpAddress(ctx.channel.localAddress)
      val remoteAddress = NettyTcpAddress(ctx.channel.remoteAddress)
      logger.checkEq(actor.address, localAddress)
      registerChannel(localAddress, remoteAddress, ctx.channel)
      ctx.fireChannelRegistered()
    }

    override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
      val localAddress = NettyTcpAddress(ctx.channel.localAddress)
      val remoteAddress = NettyTcpAddress(ctx.channel.remoteAddress)
      unregisterChannel(localAddress, remoteAddress)
      ctx.fireChannelUnregistered()
    }
  }

  private class ClientHandler(
      actor: Actor[NettyTcpTransport]
  ) extends CommonHandler(actor) {
    override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
      if (ctx.channel.localAddress != null &&
          ctx.channel.remoteAddress != null) {
        val remoteAddress = NettyTcpAddress(ctx.channel.remoteAddress)
        unregisterChannel(actor.address, remoteAddress)
        ctx.fireChannelUnregistered()
      } else {
        logger.debug(
          "Unregistering a channel with null source or destination " +
            "addresses. The connection must have failed."
        )
      }
    }
  }

  private class LogFailureFutureListener(message: String)
      extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (future.isSuccess()) {
        return
      }

      if (future.isCancelled()) {
        logger.debug(s"Future was cancelled: $message")
      } else {
        logger.debug(s"Future failed: $message")
        logger.debug(future.cause().getStackTrace.mkString("\n"))
      }
    }
  }

  private class CloseOnFailureFutureListener extends ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (!future.isSuccess()) {
        future.channel.close()
      }
    }
  }

  override type Address = NettyTcpAddress
  override def addressSerializer = NettyTcpAddressSerializer
  override type Timer = NettyTcpTimer

  // TODO(mwhittaker): The netty documentation [1] suggests using two event
  // loop groups: a "boss" group and a "worker" group. According to the
  // documentation, the boss group is responsible for establishing connections
  // while the worker group does the actual message processing. However, when
  // using two event loop groups and printing out thread ids, I found that
  // multiple threads were being used. I'm not exactly sure why this is
  // happening. I must be misunderstanding the netty API. Using a single event
  // loop, though, seems to solve the problem. With only one event loop, only
  // one thread is used.
  //
  // [1]: https://netty.io/wiki/user-guide-for-4.x.html
  private val eventLoop = new NioEventLoopGroup(1)

  sealed trait ChanOrPending
  case class Chan(channel: Channel) extends ChanOrPending
  case class Pending(msgs: mutable.Buffer[Array[Byte]]) extends ChanOrPending

  // `channels` is a little hard to understand, so bear with me. There are two
  // types of channels:
  //
  //   (1) _Server Channels_: Every actor has a unique address and runs a TCP
  //       server socket that listens on this address. When a different actor
  //       connects to this server socket, a server channel is formed. The
  //       local address of this channel is the address of the actor. The
  //       remote address will have the same host as the remote actor but will
  //       probably have a random port.
  //
  //   (2) _Client Channels_: When an actor sends a message to a different
  //       actor, it creates a client socket and connects to the remote actor's
  //       address. This creates a client channel. The local address of this
  //       channel has a random port. The remote address is the address of the
  //       remote actor.
  //
  // We do not expose these random ports to programmers. Programmers only know
  // about the addresses they give to actors. When we form a server channel, it
  // is stored in `channels` with its local and remote address. When we form a
  // client channel, we store it in `channels`, but with the local actor's
  // address rather than the client channel's local address. This is because
  // when the local actor sends a message, it doesn't even know what the random
  // port is. All it knows is its address.
  private val channels = mutable.Map[
    (NettyTcpTransport#Address, NettyTcpTransport#Address),
    ChanOrPending
  ]()

  private def registerChannel(
      localAddress: NettyTcpTransport#Address,
      remoteAddress: NettyTcpTransport#Address,
      channel: Channel
  ): Unit = {
    channels.get((localAddress, remoteAddress)) match {
      case Some(Chan(_)) =>
        logger.fatal(
          s"A channel between local address $localAddress and remote address " +
            s"$remoteAddress is being registered, but there already exists " +
            s"such a channel."
        )

      case Some(Pending(msgs)) =>
        logger.debug(
          s"A channel between local address $localAddress and remote address " +
            s"$remoteAddress is being registered, and a set of ${msgs.size} " +
            s"pending messages was found. We are sending the pending " +
            s"messages along the channel."
        )
        channels((localAddress, remoteAddress)) = Chan(channel)
        msgs.foreach(
          channel
            .write(_)
            .addListener(
              new LogFailureFutureListener(
                s"Unable to send message from $localAddress to $remoteAddress."
              )
            )
        )
        channel.flush()

      case None =>
        logger.debug(
          s"Successfully registered channel between local address " +
            s"$localAddress and remote address $remoteAddress."
        )
        channels((localAddress, remoteAddress)) = Chan(channel)
    }
  }

  private def unregisterChannel(
      localAddress: NettyTcpTransport#Address,
      remoteAddress: NettyTcpTransport#Address
  ): Unit = {
    channels.remove((localAddress, remoteAddress)) match {
      case Some(_) =>
        logger.debug(
          s"Successfully unregistered channel between local address " +
            s"$localAddress and remote address $remoteAddress."
        )

      case None =>
        logger.fatal(
          s"A channel between local address $localAddress and remote " +
            s"address $remoteAddress is being unregistered, but an existing " +
            s"channel between these addresses does't exists."
        )
    }
  }

  override def register(
      address: NettyTcpTransport#Address,
      actor: Actor[NettyTcpTransport]
  ): Unit = {
    new ServerBootstrap()
      .group(eventLoop)
      .channel(classOf[NioServerSocketChannel])
      .option[Integer](ChannelOption.SO_BACKLOG, 128)
      .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .childHandler(new ChannelInitializer[SocketChannel]() {
        @throws(classOf[Exception])
        override def initChannel(channel: SocketChannel): Unit = {
          logger.info(
            s"Server socket on address ${channel.localAddress} established " +
              s"connection with ${channel.remoteAddress}."
          )
          channel.pipeline.addLast(
            "frameDecoder",
            new LengthFieldBasedFrameDecoder(10485760, 0, 4, 0, 4)
          )
          channel.pipeline.addLast("bytesDecoder", new ByteArrayDecoder())
          channel.pipeline.addLast(new ServerHandler(actor))
          // channel.pipeline.addLast(new LoggingHandler())
          channel.pipeline.addLast("frameEncoder", new LengthFieldPrepender(4))
          channel.pipeline.addLast("bytesEncoder", new ByteArrayEncoder())
        }
      })
      .bind(address.socketAddress)
      .addListeners(
        new LogFailureFutureListener(
          s"A socket could not be bound to ${address.socketAddress}."
        ),
        new CloseOnFailureFutureListener()
      )
  }

  // TODO(mwhittaker): If we're sending a message to an address for which we
  // have an actor registered. There is no need to actually send it. We can
  // pass it directly to the actor. The one hiccup is that want to schedule the
  // sending to happen after the code calling send finishes.
  private def sendImpl(
      actor: Actor[NettyTcpTransport],
      dst: NettyTcpTransport#Address,
      bytes: Array[Byte],
      flush: Boolean
  ): Unit = {
    channels.get((actor.address, dst)) match {
      case Some(Chan(channel)) =>
        val future = if (flush) {
          channel.writeAndFlush(bytes)
        } else {
          channel.write(bytes)
        }
        future.addListener(
          new LogFailureFutureListener(
            s"Unable to send message from ${actor.address} to $dst."
          )
        )

      case Some(Pending(msgs)) =>
        logger.debug(
          s"Attempted to send a message from ${actor.address} to $dst, but " +
            s"a channel is currently pending. The message is being buffered " +
            s"for later."
        )
        msgs += bytes

      case None =>
        logger.debug(
          s"No channel was found between ${actor.address} and $dst, so we " +
            s"are creating one."
        )
        channels((actor.address, dst)) = Pending(mutable.Buffer(bytes))
        new Bootstrap()
          .group(eventLoop)
          .channel(classOf[NioSocketChannel])
          .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
          .handler(new ChannelInitializer[SocketChannel]() {
            @throws(classOf[Exception])
            override def initChannel(channel: SocketChannel) {
              channel.pipeline.addLast(
                "frameDecoder",
                new LengthFieldBasedFrameDecoder(10485760, 0, 4, 0, 4)
              )
              channel.pipeline
                .addLast("bytesDecoder", new ByteArrayDecoder())
              channel.pipeline.addLast(new ClientHandler(actor))
              // channel.pipeline.addLast(new LoggingHandler())
              channel.pipeline
                .addLast("frameEncoder", new LengthFieldPrepender(4))
              channel.pipeline
                .addLast("bytesEncoder", new ByteArrayEncoder())
            }
          })
          .connect(dst.socketAddress)
          .addListeners(
            new LogFailureFutureListener(s"Unable to connect to $dst."),
            new CloseOnFailureFutureListener(),
            new ChannelFutureListener() {
              override def operationComplete(future: ChannelFuture): Unit = {
                if (!future.isSuccess()) {
                  unregisterChannel(actor.address, dst)
                } else {
                  logger.info(
                    s"Client socket on address " +
                      s"${future.channel.localAddress} (a.k.a. " +
                      s"${actor.address}) established connection with " +
                      s"${future.channel.remoteAddress}."
                  )
                  registerChannel(actor.address, dst, future.channel)
                }
              }
            }
          )
    }
  }

  override private[frankenpaxos] def send(
      actor: Actor[NettyTcpTransport],
      dst: NettyTcpTransport#Address,
      bytes: Array[Byte]
  ): Unit =
    sendImpl(actor, dst, bytes, flush = true)

  override private[frankenpaxos] def sendNoFlush(
      actor: Actor[NettyTcpTransport],
      dst: NettyTcpTransport#Address,
      bytes: Array[Byte]
  ): Unit =
    sendImpl(actor, dst, bytes, flush = false)

  override private[frankenpaxos] def flush(
      actor: Actor[NettyTcpTransport],
      dst: NettyTcpTransport#Address
  ): Unit = {
    channels.get((actor.address, dst)) match {
      case Some(Chan(channel)) =>
        channel.flush()

      case Some(Pending(msgs)) =>
        logger.debug(
          s"Attempted to flush the channel between ${actor.address} and " +
            s"$dst, but the channel is currently pending. The flush is being " +
            s"ignored."
        )

      case None =>
        logger.debug(
          s"Attempted to flush the channel between ${actor.address} and " +
            s"$dst, but no such channel exists. The flush is being ignored."
        )
    }
  }

  override def timer(
      address: NettyTcpTransport#Address,
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): NettyTcpTransport#Timer = {
    new NettyTcpTimer(eventLoop, logger, name, delay, f)
  }

  override def executionContext(): ExecutionContext = {
    scala.concurrent.ExecutionContext.fromExecutorService(eventLoop)
  }

  def shutdown(): Future[_] = {
    eventLoop.shutdownGracefully()
  }
}
