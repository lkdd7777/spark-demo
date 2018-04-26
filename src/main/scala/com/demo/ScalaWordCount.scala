package com.demo

import org.apache.spark.
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {

  def main(args: Array[String]): Unit = {
      if(args.length < 1){
          System.err.println("file not exists")
          System.exit(1)
      }

      var conf = new SparkConf().setAppName("wordCount").setMaster("local")
      var sc = new SparkContext(conf)
      var textFile  = sc.textFile(args(0),1)

      var counts = textFile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

      counts.sortByKey().take(20).foreach(println)

//    textFile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)
      sc.stop()
  }
}
