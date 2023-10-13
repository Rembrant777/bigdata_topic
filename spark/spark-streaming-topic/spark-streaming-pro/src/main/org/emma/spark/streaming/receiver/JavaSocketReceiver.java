package org.emma.spark.streaming.receiver;

import com.google.common.io.Closeables;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * To make the spark streaming unit test cases executes as expected (consuming data from socket connection)
 * Here we define a customized spark streaming receiver.
 * And this code also refers to the spark source code:
 * org.apache.spark.streaming.JavaReceiverAPISuite.JavaSocketReceiver#JavaSocketReceiver
 */
public class JavaSocketReceiver extends Receiver<String> {
    private String host = null;
    private int port = -1;

    public JavaSocketReceiver(String host_, int port_) {
        super(StorageLevel.MEMORY_AND_DISK());
        host = host_;
        port = port_;
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {

    }

    private void receive() {
        try {
          Socket socket = null;
          BufferedReader in = null;

          try {
              socket = new Socket(host, port);
              in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
              String userInput;
              while ((userInput = in.readLine()) != null) {
                  store(userInput);
              }
          } finally {
              Closeables.close(in, true);
              Closeables.close(socket, true);
          }
        } catch (ConnectException ce) {
            ce.printStackTrace();
            restart("Could not connect", ce);
        } catch (Throwable t) {
            t.printStackTrace();
            restart("Error receiving data ", t);
        }
    }
}
