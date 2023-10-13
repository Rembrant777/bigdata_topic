package com.emma.netty.bio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Objects;

public class TimeClient {

    public static void main(String[] args) {
        int port = 8080;
        if (args != null && args.length > 0) {
            try {
                port = Integer.valueOf(args[0]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        Socket socket = null;
        BufferedReader in = null;
        PrintWriter out = null;

        try {
            // client side try to create connection to the server side via the Socket instance
            socket = new Socket("localhost", port);

            // create input stream to retrieve message from the server side
            in = new BufferedReader(new InputStreamReader(
                    socket.getInputStream()));

            // create output stream to send message request to the server side
            out = new PrintWriter(socket.getOutputStream(), true);

            // client side try to establish communciation by first sending the query time message content.
            out.println("QUERY TIME ORDER");

            // then waits for the server side's response , retrieve message content string by readLine method
            String resp = in.readLine();

            // print on the console
            System.out.println("Now is :" + resp);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (Objects.nonNull(out)) {
                out.close();
            }
            if (Objects.nonNull(in)) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (Objects.nonNull(socket)) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }
    }
}
