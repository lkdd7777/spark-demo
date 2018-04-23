package com.demo;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.spark_project.guava.collect.Lists;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        Set<String> set = new HashSet<String>();
        set.add("suitable");
        set.add("against");
        set.add("recent");

        if(args.length < 1){
            System.out.println("file not exists");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        conf.setAppName("JavaWorkCount");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0],1);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                int i = 0;

                List<String> list = Arrays.asList(SPACE.split(s));
//                for (Iterator iter = list.iterator(); iter.hasNext();) {
//                    String str = iter.next().toString();
//
//                    if(!set.contains(str)){
//                        list.remove(i);
//                    }
//                    i++;
//                }
                return list.iterator();
            }
        });

        JavaPairRDD<String,Integer> wordcount = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String,Integer>> output = wordcount.collect();

        for (Tuple2<?,?> tuple : output){
            System.out.println(tuple._1() + ":" + tuple._2());
        }

        sc.close();
    }
}
