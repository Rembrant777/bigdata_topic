package org.emma.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext$;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName("WordCount")
                .set("spark.serializer", KryoSerializer.class.getName());

        // batch interval 5ms
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        // here we define the socket where the system will listen
        // and here the lines return by the jssc#socketTextStream is not rdd but a sequence of rdd, not static, constantly changing.
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[0], Integer.parseInt(args[1]));

        // split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.stream(s.split(" ")).iterator();
            }
        });

        // count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(item -> new Tuple2<>(item, 1));

        // aggregate all the sum of each batch --> and this batch is different from previous windows or with the state.
        // this batch only in the scope of one Duration
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((val1, val2) -> (val1 + val2));

        // here we print the first ten elements of each RDD generated in this DStream to the console
        jssc.start();
        jssc.awaitTermination();
    }
}
