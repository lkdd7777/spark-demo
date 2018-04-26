package com.demo
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
object Record {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

  }

}
