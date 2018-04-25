package com.demo

import org.apache.spark.sql.SparkSession

object ScalaSparkHive {

  def main(args: Array[String]): Unit = {
      var spark =  SparkSession.builder().appName("Scala Spark Hive")
          .config("spark.some.config.option","some-value").getOrCreate()

      spark.sql("show tables")
      spark.sql("desc orders")

      spark.stop()
  }
}
