//package com.emma.netty.codec.protobuf;
//
//import io.netty.bootstrap.Bootstrap;
//import io.netty.channel.ChannelFuture;
//import io.netty.channel.ChannelInitializer;
//import io.netty.channel.EventLoopGroup;
//import io.netty.channel.nio.NioEventLoopGroup;
//import io.netty.channel.socket.SocketChannel;
//import io.netty.channel.socket.nio.NioSocketChannel;
//import io.netty.handler.codec.protobuf.ProtobufDecoder;
//import io.netty.handler.codec.protobuf.ProtobufEncoder;
//import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
//import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class SubReqClient {
//    private static final Logger LOG = LoggerFactory.getLogger(SubReqClient.class);
//
//    public void connect(int port, String host) throws Exception {
//        // configure client NIO thread group
//        EventLoopGroup group = new NioEventLoopGroup();
//        try {
//            Bootstrap b = new Bootstrap();
//            b.group(group).channel(NioSocketChannel.class)
//                    .handler(new ChannelInitializer<SocketChannel>() {
//                        @Override
//                        protected void initChannel(SocketChannel socketChannel) throws Exception {
//                            // todo: trying to figure out the order of the SocketChannel add to pipeline
//                            // how to effect on the received messages
//                            socketChannel.pipeline().addLast(new ProtobufVarint32FrameDecoder());
//                            socketChannel.pipeline().addLast(new ProtobufDecoder(SubscribeReqProto.SubscribeResp.getDefaultInstance()));
//                            socketChannel.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
//                            socketChannel.pipeline().addLast(new ProtobufEncoder());
//                            socketChannel.pipeline().addLast(new SubReqClientHandler());
//                        }
//                    });
//            // here establish the async connection
//            ChannelFuture future = b.connect(host, port).sync();
//
//            // close current client side's link
//            future.channel().closeFuture().sync();
//        } finally {
//            LOG.info("#connect exit gracefully, and release the thread group of NIO");
//            group.shutdownGracefully();
//        }
//    }
//
//    // I have an idea here (maybe not a good one, hahahaha)
//    // why don't we try to let spring context load this SubReqClient via its class instead of
//    // use main to set up the client side's process ?
//}
