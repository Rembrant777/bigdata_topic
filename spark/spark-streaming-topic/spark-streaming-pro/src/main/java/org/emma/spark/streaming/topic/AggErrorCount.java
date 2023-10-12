package org.emma.spark.streaming.topic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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
import java.util.List;

public class AggErrorCount implements Serializable {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName("AggErrorCount")
                .set("spark.serializer", KryoSerializer.class.getName());

        // here set batch interval 5ms
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

        // set checkpoint, NEEDED for update state by key operation
        jssc.checkpoint("tmp/spark");

        // define the socket where the system will listen lines
        // is not a rdd but a sequence of rdd, not static, constantly changing
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[0], Integer.parseInt(args[1]));

        // split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.stream(s.split(" ")).iterator();
            }
        });

        // filter using the lines containing errors
        JavaDStream<String> filterWords = lines.filter(item -> item.contains("ERROR"));

        // here convert each words into pairs in which key is the word,
        // value is the init count in the context of the streaming
        JavaPairDStream<String, Integer> pairs = filterWords.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                }
        );

        // aggregates all values in the global context instead of
        // in the grain of 1000s windows period.

        // the value of the state in the function is referring to the number of the micro-batch that with the step of the 1000s duration window.
        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newSum = values.stream().reduce(0, Integer::sum);
                if (state != null && state.isPresent()) {
                    newSum += state.get();
                }
                return Optional.of(newSum);
            }
        });

        // in remain codes will try write the datasets to other middle wares like kfk, es..
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
