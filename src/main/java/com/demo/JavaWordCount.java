package com.demo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;

public class JavaWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("JavaWorkCount");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(args[0],1);

        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
//        counts.saveAsTextFile("hdfs://master:9000/output/result.txt");
        counts.foreach(t -> System.out.println(t._1() + ":" + t._2()));

        sc.close();
    }
}
