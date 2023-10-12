package org.emma.spark.streaming.topic

import org.apache.spark.sql.SparkSession

trait SparkSessionITWrapper {
  lazy val spark: SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Integration Test Wrapper")
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    spark
  }
}
