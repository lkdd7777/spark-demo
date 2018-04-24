package com.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * SparkStreaming test
 */
public class JavaSparkStreamTest {

    public static void main(String[] args)throws Exception {
        SparkConf conf = new SparkConf().setAppName("JavaSparkStreamTest").setMaster("local[2]");
//        JavaStreamingContext ssc = new JavaStreamingContext(conf,new Duration(1000));
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> line = ssc.socketTextStream("locahost",9999);

        JavaDStream<String> words = line.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairDStream<String,Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s,1)).reduceByKey((a,b) -> a+b);

        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
