package org.emma.spark.streaming;

import com.twitter.chill.KryoSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class BasicErrorCount implements Serializable {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("")
                .setAppName("BasicErrorCount")
                .set("spark.serializer", KryoSerializer.class.getName());

        // set batch interval 5ms
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        // define the socket where the system will listen to
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[0], Integer.parseInt(args[1]));

        // split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.stream(s.split("  ")).iterator();
            }
        });


        // filter using the lines containing errors
        JavaDStream<String> filteredWords = lines.filter(word -> word.contains("ERROR"));

        // then count each word in each batch
        JavaPairDStream<String, Integer> pairs = filteredWords.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                // here set the init count value for inputting string value
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // aggregate each value
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer count1, Integer count2) throws Exception {
                return (count1 + count1);
            }
        });

        // here print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
