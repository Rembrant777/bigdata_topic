package org.emma.spark.streaming.topic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class WindowAndKeyErrorCount implements Serializable {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName("WindowAndKeyErrorCount")
                .set("spark.serializer", KryoSerializer.class.getName());



        // batch interval 5ms
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));


        // here we set path that to let the spark streaming context know where the checkpoint locates
        // increase the spark job restart to re-consume dataset from

        // CHECKPOINT, NEEDED FOR UPDATE STATE BY KEY OPERATION
        jssc.checkpoint("tmp/spark");

        // all dataset is consumed from the socket listener
        // lines is not a rdd but a sequence of rdd, not static, constantly chaning
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[0], Integer.parseInt(args[1]));

        // split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.stream(s.split(" ")).iterator();
            }
        });

        // filter using the lines containing errors
        JavaDStream<String> filteredWords = lines.filter(word -> word.contains("ERROR"));

        // count each word in each batch
        JavaPairDStream<String, Integer> pairs = filteredWords.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // here we execute reduce operation upon each rdds from the DStream
        // here we create a window which accumulates from multiple DStream that with the duration step value = 5 milliseconds
        // which means we collect small fractions of DStream items into Windows, and re-organize the DStreams in the period of Windows.

        // and execute reduce operation in the grain of window

        // the first params of the duration is the window duration which means the length of the window.
        // and the second param of the duration is the slide step length

        // e.g. if we set the window length = 5 seconds, which means the window contains all the DStreams that [0, 5]
        // and with the slide window length = 10, if the first window is [0,5] the next window is [10, 15] [w_start + slide_window_len, w_end + slide_window_len]
        // and always keep the w_end - w_start = 5
        JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer val1, Integer val2) throws Exception {
                return (val1 + val2);
            }
        }, Durations.seconds(5), Durations.seconds(10));

        // print the first ten elements of each RDD generated in this DStream to the console
        windowedWordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
