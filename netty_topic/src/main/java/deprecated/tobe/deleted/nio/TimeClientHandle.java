package deprecated.tobe.deleted.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class TimeClientHandle implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(TimeClientHandle.class);

    private String host;
    private int port;

    private Selector selector;
    private SocketChannel socketChannel;
    private volatile boolean stop;

    public TimeClientHandle(String localhost, int port) {
        this.host = localhost;
        this.port = port;

        try {
            // here ! we create ! Reactor Thread !!!
            selector = Selector.open();
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
        } catch (IOException ie) {
            ie.printStackTrace();
            System.exit(1);
        }
    }


    @Override
    public void run() {
        try {
            doConnect();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        while (!stop) {
            try {
                selector.select(1000);
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> it = selectionKeys.iterator();
                SelectionKey key = null;

                while (it.hasNext()) {
                    key = it.next();
                    it.remove();
                    try {
                        handleInput(key);
                    } catch (Exception ex) {
                        if (Objects.isNull(key)) {
                            key.cancel();
                            if (Objects.nonNull(key.channel())) {
                                key.channel().close();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        // when multiplexer close all registered Channel and Pipes will be released
        // and closed automatically.
        if (Objects.nonNull(selector)) {
            try {
                selector.close();
            } catch (IOException ie) {
                ie.printStackTrace();
            }
        }
    }

    private void doConnect() throws IOException {
        if (socketChannel.connect(new InetSocketAddress(host, port))) {
            socketChannel.register(selector, SelectionKey.OP_READ);
            doWrite(socketChannel);
        } else {
            socketChannel.register(selector, SelectionKey.OP_CONNECT);
        }
    }

    private void doWrite(SocketChannel sc) throws IOException {
        byte[] req = "QUERY TIME ORDER".getBytes();
        ByteBuffer writeBuffer = ByteBuffer.allocate(req.length);
        writeBuffer.put(req);
        // position <- 0
        // limit <- len(req)
        writeBuffer.flip();

        sc.write(writeBuffer);
        if (!writeBuffer.hasRemaining()) {
            LOG.info("#doWrite send order to server succeed.");
        }
    }

    private void handleInput(SelectionKey key) throws IOException {
        LOG.info("#handleInput recv key {}", key.isValid());
        if (key.isValid()) {
            // check connection is ok
            SocketChannel sc = (SocketChannel) key.channel();
            if (key.isConnectable()) {
                // isConnectable is OP_CONNECT state validation
                if (sc.finishConnect()) {
                    LOG.info("#handleInput connection success");
                    sc.register(selector, SelectionKey.OP_READ);
                    doWrite(sc);
                } else {
                    LOG.warn("#handleInput connect failed, return!");
                    System.exit(-1);
                }
            }

            if (key.isReadable()) {
                // isConnectable is OP_READ state validation
                ByteBuffer readBuffer = ByteBuffer.allocate(1024);
                int readBytes = sc.read(readBuffer);
                if (readBytes > 0) {
                    readBuffer.flip();
                    byte [] bytes = new byte [readBuffer.remaining()];
                    readBuffer.get(bytes);
                    String body = new String(bytes, "UTF-8");
                    LOG.info("#handleInput content : {}", body);
                    if (Objects.isNull(body) || body.trim().length() == 0 || body.contains("BAD")) {
                        LOG.info("#handleInput content {}", body);
                    } else {
                        LOG.info("#handleInput content {}", body);
                        System.out.println("#handleInput content: " +  body);
                        this.stop = true;
                    }
                } else if (readBytes < 0) {
                    key.cancel();
                    sc.close();
                } else {
                    // skip, read 0 byte just ignore
                }
            }
        } else {
            LOG.warn("#handleInput SelectionKey is invalid, return!");
            return;
        }
    }
}
