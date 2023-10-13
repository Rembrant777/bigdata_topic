package org.emma.spark.streaming;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import org.emma.spark.streaming.receiver.JavaSocketReceiver;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Java Spark Streaming Suite Codes refered from
 * org.apache.spark.streaming.JavaReceiverAPISuite from spark 3.5 source code.
 */
public class JavaReceiverAPISuite implements Serializable {
    @Before
    public void setUp() {
        System.clearProperty("spark.streaming.clock");
    }

    @After
    public void tearDown() {
        System.clearProperty("spark.streaming.clock");
    }

    @Test
    public void testReceiver() throws InterruptedException {
        // in each unit test case, we setup TestServer first
        TestServer testServer = new TestServer(0);
        testServer.start();

        AtomicLong dataCounter = new AtomicLong(0);

        try {
            JavaStreamingContext jssc = new JavaStreamingContext("local[2]", "JavaStreamingTestApp", new Duration(200));
            JavaReceiverInputDStream<String> inputDStream =
                    jssc.receiverStream(new JavaSocketReceiver("localhost", testServer.port()));
            JavaDStream<String> mappedDStream = inputDStream.filter(Objects::nonNull).map(msg -> String.format("%s.", msg));
            mappedDStream.foreachRDD(rdd -> {
                long count = rdd.count();
                dataCounter.addAndGet(count);
            });

            jssc.start();
            long startTimeNs = System.nanoTime();
            long timeout = TimeUnit.SECONDS.toNanos(10);

            Thread.sleep(200);
            for (int i = 0; i < 6; i++) {
                // here call test server's send operaiton
                // to feed test server's inner queue
                // and when test server established the connection to the clien t
                // and also finds out that the queue contains elements of characters
                // it will send the contents from the queue to the client via the socket connection


                // and the socket connection is connected to the client endpoint that we created
                // in the spark streaming's receiver side, in this way,
                // the receiver consumeds the character via the socket from the serverside
                //then passing it to the current JavaSparkStreaming's retrieved DStream to execute the word count operation
                // to update the global; thread-safe dataCounter

                // and once the dataCounter collect enough characters it will invoke shutdown spark streaming job
                testServer.send(i + "\n"); // append \n here to make sure these are separated lines
                Thread.sleep(100);
            }

            while (dataCounter.get() == 0 && System.nanoTime() - startTimeNs < timeout) {
                Thread.sleep(100);
            }

            jssc.stop();
            Assert.assertTrue(dataCounter.get() > 0);
        } finally {
            testServer.stop();
        }
    }
}
