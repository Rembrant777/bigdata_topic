package org.emma.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class WindowErrorCount implements Serializable {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName("WindowErrorCount")
                .set("spark.serializer", KryoSerializer.class.getName());

        // batch interval 5ms
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

        // CHECKPOINT, NEEDED FOR UPDATE STATE BY KEY OPERATION
        jssc.checkpoint("tmp/spark");

        // define the socket where the system will listen
        // And the lines is not a rdd but a sequence of rdd, not static, constantly changing
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[0], Integer.parseInt(args[1]));

        // here we split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.stream(s.split(" ")).iterator();
            }
        });

        // filter using the lines containing errors
        JavaDStream<String> filteredWords = lines.filter(item -> item.contains("ERROR"));

        // count each word in each batch
        JavaPairDStream<String, Integer> pairs = filteredWords.mapToPair(item -> new Tuple2<>(item, 1));

        // window count USING THE SIZE AND THE SLIDIND INTERVAL
        JavaDStream<Long> wordCounts = pairs.countByWindow(new Duration(5000), new Duration(10000));

        // print the first ten elements of each RDD generated in the DStream to the console
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
