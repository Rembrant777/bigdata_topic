package deprecated.tobe.deleted.nio;

import java.util.Objects;

public class TimeServer {
    public static void main(String[] args) {
        int port = 8080;
        if (Objects.nonNull(args) && args.length > 0) {
            try {
                port = Integer.valueOf(args[0]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        MultiplexerTimeServer timeServer = new MultiplexerTimeServer(port);
        new Thread(timeServer, "NIO-MultiplexerTimeServer-001").start();
    }
}
