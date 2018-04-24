package com.demo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.util.*;

public class JavaWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("JavaWorkCount");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        HashSet<String> set = new HashSet<>();
        set.add("suitable");
        set.add("against");
        set.add("recent");
        Broadcast<Set<String>> broadcastVar = sc.broadcast(set);

        JavaRDD<String> textFile = sc.textFile(args[0],1);

        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s ->Arrays.asList(s.split(" ")).iterator())//行切割成单词
                .filter(s -> broadcastVar.value().contains(s))//过滤set
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
//        counts.saveAsTextFile("hdfs://master:9000/output/result.txt");
        counts.sortByKey().take(20).forEach(t -> System.out.println(t._1() + ":" + t._2()));
        sc.close();
    }
}
