package org.emma.spark.streaming.app;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.emma.spark.streaming.converter.JsonConverter;
import org.emma.spark.streaming.internal.TestServer;
import org.emma.spark.streaming.utils.RDDUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class AppErrorCountJavaStreamingAppSuite implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(AppErrorCountJavaStreamingAppSuite.class);

    @Before
    public void setUp() {
        System.clearProperty("spark.streaming.clock");
    }

    @After
    public void tearDown() {
        System.clearProperty("spark.streaming.clock");
    }

    @Test
    public void testAppErrorCountJavaStreamingApp() throws InterruptedException {
        // Create a TestServer to generate upstreaming data
        TestServer testServer = new TestServer(0);
        testServer.start();

        // --- here add Spark Streaming App Logic ---
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("AppErrorCountApp")
                .set("spark.serializer", KryoSerializer.class.getName());
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        jssc.checkpoint("/tmp/AppErrorCountApp/checkpoint");

        JavaReceiverInputDStream<String> dStreamLines = jssc
                .socketTextStream("localhost", testServer.port());


        dStreamLines.map(str -> {
                    if (Objects.nonNull(str) && str.length() > 0) {
                        return Arrays.stream(str.split(" ")).iterator();
                    } else {
                        return null;
                    }
                }).flatMap(item -> item)
                .mapToPair(item -> new Tuple2<>(item, 1))
                .updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                        Integer newSum = values.stream().reduce(0, Integer::sum);
                        if (state != null && state.isPresent()) {
                            newSum += state.get();
                        }
                        return Optional.of(newSum);
                    }
                })
                .filter(item -> Objects.nonNull(item) && Objects.nonNull(item._2) && Objects.nonNull(item._1) && item._2 > 0)
                .map(item -> {
                    SumRecordItem itemRecord = new SumRecordItem(Long.valueOf(item._2), item._1);
                    return itemRecord;
                }).foreachRDD(rdd -> {
                    final Map<Integer, Iterator<SumRecordItem>> cache = new HashMap<>();
                    rdd.foreachPartition(partition -> {
                        int partitionId = TaskContext.get().partitionId();
                        LOG.info("#foreachPartition partition id {}, partition iterator non-null status {}, has next status {}",
                                partitionId, Objects.nonNull(partition), partition.hasNext());
                        cache.put(partitionId, partition);
                    });
                    LOG.info("#foreachRDD recv map len {}", cache.size());
                    for (Map.Entry<Integer, Iterator<SumRecordItem>> entry : cache.entrySet()) {
                        RDDUtils.writeToLocalDisk("file://tmp/output/data", entry.getValue(), new JsonConverter<SumRecordItem>());
                    }
                });


        // --- here add Spark Streaming App Logic ---

        // here check whether test server setup ok
        Assertions.assertTrue(Objects.nonNull(testServer) && testServer.startState());

        Thread.sleep(200);
        // feed test server with Spark App's required dataset format pattern
        // feed data will be stored in the TestServer's queue first
        int totalMsgCnt = 10;
        for (int i = 0; i < totalMsgCnt; i++) {
            testServer.send(String.format("ERROR: number index %s content %s", i, UUID.randomUUID().toString()));
            Thread.sleep(100);
        }

        // here check the Spark Streaming App's retrieved results and check

        Assertions.assertTrue(true);
        // end the App
        jssc.start();
        jssc.awaitTermination();

        LOG.info("Close TestServer that listen on port {}", testServer.port());
    }

    public static class SumRecordItem implements Serializable {
        public Long sumCnt;
        public String strContent;

        public SumRecordItem(Long sumCnt, String strContent) {
            this.sumCnt = sumCnt;
            this.strContent = strContent;
        }
    }
}
