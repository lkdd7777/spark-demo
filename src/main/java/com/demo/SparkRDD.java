package com.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRDD {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("F:/code/spark-demo/The_Man_of_Property.txt",1);

        JavaRDD<Integer> lineLengths = textFile.map(s -> s.length());
        int totalLnegth = lineLengths.reduce((a,b) -> a+b);

        System.out.println(totalLnegth);
    }
}
