package com.emma.netty.bio;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;

public class TimeServer {
    public static void main(String[] args) throws IOException {
        int port = 8080;
        if (Objects.nonNull(args) && args.length > 0) {
            try {
                port = Integer.valueOf(args[0]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        ServerSocket server = null;
        try {
            server = new ServerSocket(port);
            System.out.println("Time server is started and listen to port: " + port);
            Socket socket = null;
            while (true) {
                // server accept client side request as Socket instance
                socket = server.accept();

                // passing Socket instance to handler
                // handler will create a new thread and execute to process the request(Socket) in the scope of the thread.
                new Thread(new TimeServerHandler(socket)).start();
            }
        } finally {
            if (server != null) {
                System.out.println("Time server shutdown && close!");
                server.close();
                server = null;
            }
        }
    }
}
