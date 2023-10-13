package com.emma.netty.bio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Date;
import java.util.Objects;

public class TimeServerHandler implements Runnable {
    private Socket socket;

    public TimeServerHandler(Socket socket) {
        this.socket = socket;
    }

    // specific handle logic implemented in the run method
    @Override
    public void run() {
        BufferedReader in = null;
        PrintWriter out = null;
        try {
            // first create reader from the socket instance to retrieve client side's message
            in = new BufferedReader(new InputStreamReader(
                    this.socket.getInputStream()));

            // then build output stream which to send messages to the client side
            out = new PrintWriter(this.socket.getOutputStream(), true);

            String currentTS = null;
            String body = null;

            while (true) {
                // begin to retrieve && parse client side's request message by read line
                body = in.readLine();
                if (Objects.isNull(body)) {
                    break;
                }
                System.out.println("The time server receive order: " + body);

                // try to verify client's request content match with specific message content
                currentTS = "QUERY TIME ORDER".equalsIgnoreCase(body) ?
                        new Date(System.currentTimeMillis()).toString() : "BAD ORDER";

                // write the corresponding content via the output stream to send request body to the client side
                out.println(currentTS);
            }
        } catch (Exception e) {
            if (Objects.nonNull(in)) {
                try {
                    in.close();
                } catch (IOException ie) {
                    ie.printStackTrace();
                }
            }

            if (Objects.nonNull(out)) {
                out.close();
            }

            if (Objects.nonNull(this.socket)) {
                try {
                    this.socket.close();
                } catch (IOException ie) {
                    ie.printStackTrace();
                }
                this.socket = null;
            }
        }
    }
}
