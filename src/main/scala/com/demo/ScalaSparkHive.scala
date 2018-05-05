package com.demo


import org.apache.spark.sql.SparkSession

object ScalaSparkHive {

	//./bin/spark-submit --class com.demo.ScalaSparkHive --master spark://master:7077 /mnt/hgfs/code/spark-demo/out/artifacts/scala-spark-hive/spark-demo.jar
	//./bin/spark-shell --master yarn-client --jars jars/mysql-connector-java-5.1.44-bin.jar
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
				.builder()
				.appName("Spark Hive Example")
				.enableHiveSupport()
				//        .config("spark.sql.warehouse.dir", warehouseLocation)
				.getOrCreate()

		import spark.sql

		sql("show databases").show()
		sql("use mydb")
		sql("show tables").show()

		val ds = sql("select order_id ,user_id from orders limit 10")
		ds.cache()
		print("===============================================" + ds.collect().length)
		ds.select("order_id", "user_id").collect().foreach(print)
		spark.stop()
	}
}
