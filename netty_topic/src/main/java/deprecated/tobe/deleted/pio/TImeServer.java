package deprecated.tobe.deleted.pio;

import deprecated.tobe.deleted.bio.TimeServerHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;

public class TImeServer {
    public static void main(String[] args) throws IOException {
        int port = 8080;
        if (Objects.isNull(args) && args.length > 0) {
            try {
                port = Integer.valueOf(args[0]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        ServerSocket server = null;

        try {
            server = new ServerSocket(port);
            System.out.println("Time Server listen on port " + port);
            // retrieve the instance of socket via #accept method
            // via the socket we can build input/output streaming
            // to retrieve request data and write response data to the client side
            Socket socket = null;
            TimeServerHandlerExecutePool singleExecutor =
                    new TimeServerHandlerExecutePool(20, 10000);

            while (true) {
                // Why do we call this combination of a queue and threads "fake asynchronous"?
                // This is because the 'accept' method acts as a blocking point where all the threads
                // must be synchronized within this method.
                // No single thread can change the order in the queue to retrieve the accepted sockets in a disorderly manner.
                // ...
                // actually it is not the accept raise the 'synchronzied'
                // it is the io operation of thec client.
                // suppose one client connect and exchange data via server , and the server need to get blocked
                // to wait for the one of the specific client to write response
                // at the same time other remained clients have to blocked and wait
                socket = server.accept();
                // same logic that implemented in the TimeServerHandler: that every time receives a 'QUERY TIME ORDER'
                // it will write a timestamp value and sent to the client side.
                // if string not match, write a BAD ORDER in turn
                singleExecutor.execute(new TimeServerHandler(socket));
            }
        } finally {
            if (Objects.nonNull(server)) {
                System.out.println("TimeServer is close");
                server.close();
            }
        }
    }
}
