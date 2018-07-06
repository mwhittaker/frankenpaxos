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

class NettyTcpTransport extends Transport[NettyTcpTransport] {
  case class NettyTcpAddress(socketAddress: SocketAddress) extends zeno.Address

  class NettyTcpTimer extends zeno.Timer {
    def name(): String = ???
    def start(): Unit = ???
    def stop(): Unit = ???
    def reset(): Unit = ???
  }

  type Address = NettyTcpAddress
  type Timer = NettyTcpTimer

  class ServerHandler(private val actor: Actor[NettyTcpTransport])
      extends ChannelInboundHandlerAdapter {
    override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = { // (2)
      msg match {
        case buf: ByteBuf => {
          actor.receive(
            NettyTcpAddress(ctx.channel.localAddress),
            buf.toString(io.netty.util.CharsetUtil.US_ASCII)
          )
        }
        case _ =>
          // TODO(mwhittaker): Handle more gracefully.
          ???
      }
    }
  }

  // Map from (src, dst) addresses to channel
  // queue of timers
  private val eventLoop: EventLoopGroup = new NioEventLoopGroup(1);
  private var serverBootstrap = new ServerBootstrap();
  private val actors =
    new HashMap[NettyTcpTransport#Address, Actor[NettyTcpTransport]]();

  // try {
  //   var b = new ServerBootstrap();
  //
  //   // Bind and start to accept incoming connections.
  //   val channels = new scala.collection.mutable.ListBuffer[Channel]();
  //   for (port <- Seq(8000, 8001, 8002, 8003)) {
  //     var f: ChannelFuture = b.bind(port).sync();
  //     println(s"Server listening on port $port.");
  //     channels += f.channel();
  //   }
  //
  //   // Wait until the server sockets are closed.
  //   for (channel <- channels) {
  //     channel.closeFuture().sync();
  //   }

  // Constructor
  serverBootstrap
    .group(eventLoop)
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
    serverBootstrap.bind(address.socketAddress).sync();
    Try(())
  }

  def timer(duration: java.time.Duration): NettyTcpTransport#Timer = ???

  def send(
      src: NettyTcpTransport#Address,
      dst: NettyTcpTransport#Address,
      msg: String
  ): Unit = ???

  def run(): Unit = {}
}
