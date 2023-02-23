package com.geosynthesis.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mrgps {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mrgps").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql


    val data = "C:\\Users\\sajadhav\\IdeaProjects\\SparkPoc\\4G_DemoMDT_mrgps_2231_20221127-195328_20221127-211204_import.csv"

    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data)

    df.printSchema()


    df.createOrReplaceTempView("tab")
    val res = spark.sql("select LATITUDE,LONGITUDE from tab")
    res.show()


    spark.stop()
  }
}