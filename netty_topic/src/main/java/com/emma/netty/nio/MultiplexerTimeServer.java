package com.emma.netty.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class MultiplexerTimeServer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MultiplexerTimeServer.class);
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private volatile boolean stop;

    MultiplexerTimeServer(int port) {
        try {
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();

            // non-blocking settting
            serverSocketChannel.configureBlocking(false);

            // bind port to socket server to let it listen to
            // with the back-log queue len = 1024
            serverSocketChannel.socket().bind(new InetSocketAddress(port), 1024);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            LOG.info("#MultiplexerTimeServer started and listen on port {}", port);
        } catch (IOException ie) {
            LOG.error("#MultiplexerTimeServer setup failed with exp: ", ie);
        }
    }


    public void stop() {
        this.stop = true;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                // set selector select operation's timeout 1000s, in the given period any client's SelectionKey#OP_ACCEPT
                selector.select(1000);

                // retrieve the set of accept operation's keys in a set
                Set<SelectionKey> selectionKeys = selector.selectedKeys();

                // build iterator based on the selection key set
                Iterator<SelectionKey> it = selectionKeys.iterator();
                SelectionKey key = null;

                // traverse each element in the selection key via iterator
                while (it.hasNext()) {
                    // retrieve key of selection via iterator
                    key = it.next();

                    // remove the key from the iterator
                    it.remove();

                    // passing the key into the handler to handle it by pre-defined logic
                    try {
                        handleInput(key);
                    } catch (Exception e) {
                        // if there are any exceptions thrown inside the handleInput method
                        // in the catch logic will cancel current selection key's next associated operations
                        // and also close the channel associated with the current selection key
                        LOG.error("#T-{} handle selection key type {}, " +
                                        "failed in handler by exp going to be canceled(remove the key from the key set" +
                                        " which means it can be re-registered) ",
                                Thread.currentThread().getName(), SelectionKey.OP_ACCEPT, e);
                        if (Objects.nonNull(key)) {
                            key.cancel();
                            if (Objects.nonNull(key.channel())) {
                                key.channel().close();
                            }
                        }
                    }
                }

            } catch (Throwable e) {
                LOG.error("#T-{} exception", Thread.currentThread().getName(), e);
            }
        } // while

        // when multiplexer close, all the Channels and Pipe that registered on it will be
        // removed and closes, so there is no need to close and release the resources manually
        if (Objects.nonNull(selector)) {
            try {
                selector.close();
            } catch (IOException e) {
                LOG.error("#T-{} close selector failed by exception", Thread.currentThread().getName(), e);
            }
        }
    } // run

    private void handleInput(SelectionKey key) throws IOException {
        if (key.isValid()) {
            // handle new received request message
            if (key.isAcceptable()) {
                /// accept the new connection
                ServerSocketChannel ssc = (ServerSocketChannel) key.channel();

                // retrieve socket channel from socket server channel 's accept method
                SocketChannel sc = ssc.accept();

                // set non-block to the socket channel
                sc.configureBlocking(false);

                // add new connection to the selector
                sc.register(selector, SelectionKey.OP_READ);
            } // handle accept operation

            // cuz, in the accept key selection's process logic, if there are clients try to get connect to the server
            // and retrieve by the server socket's accept method, this socket channel will be registered into selector with the type of
            // SelectionKey#OP_READ
            // so here there is possible to retrieve the SelectionKey that with the type of SelectionKey#OP_READ
            if (key.isReadable()) {
                // read the data
                SocketChannel sc = (SocketChannel) key.channel();

                // allocate 1024 bytes space for new created ByteBuffer instance
                // limit <- 1024
                ByteBuffer readBuffer = ByteBuffer.allocate(1024);

                // read data from the socket's channel space to the ByteBuffer's inner storage space
                int readBytes = sc.read(readBuffer);

                if (readBytes > 0) {
                    // position <- 0
                    // limit <- readBytes len
                    readBuffer.flip();

                    // remaining <- limit - position
                    byte [] bytes = new byte [readBuffer.remaining()];
                    // data from channel -> ByteBuffer -> byte [] bytes array
                    readBuffer.get(bytes);
                    String body = new String(bytes, "UTF-8");
                    LOG.info("#T-{} TimeServer receiver order {}", body);
                    String currentTS = "QUERY TIME ORDER".equalsIgnoreCase(body)
                            ? new Date(System.currentTimeMillis()).toString() : "BAD ORDER";
                    doWrite(sc, currentTS);
                } else if (readBytes < 0) {
                    // close channel
                    LOG.info("#T-{}  close channel", Thread.currentThread().getName());
                    key.cancel();
                    sc.close();
                } else {
                    LOG.info("#T-{} read 0 byte ignore");
                }
            }
        }
    }

    private void doWrite(SocketChannel channel, String response) throws IOException {
        if (Objects.nonNull(response) && response.trim().length() > 0) {
            // convert string into byte []
            byte [] byteArr = response.getBytes();

            // allocate byte array size capacity to ByteBuffer
            ByteBuffer writeBuffer = ByteBuffer.allocate(byteArr.length);

            // move the data content from the byte array to the ByteBuffer's inner storage space
            writeBuffer.put(byteArr);

            // Flips this buffer. The limit is set to the current position and then the position is set to zero.
            // If the mark is defined then it is discarded.
            writeBuffer.flip();

            // write the ByteBuffer to the channel
            channel.write(writeBuffer);
        }
    }
}
