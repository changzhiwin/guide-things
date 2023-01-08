package zhiwin.spark.guide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.apache.logging.log4j.scala.Logging

/**
  * 如何使用Spark实现TopN的获取（ByKey的topN）
  * 注意的是topN是一个模糊定义，有三种不同结果，对应三个函数：rank/dense_rank/row_number
  * userId,score
  */

object EachKeyTopN extends Logging {

  def run(): Unit = {
    val spark = SparkSession.builder().appName("Each Key TopN").getOrCreate()
    
    val df = spark
      .read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load("hdfs://192.168.31.130:9000/spark/demo/scores.csv")

    df.printSchema()

    val windowSpec = Window.partitionBy("userId").orderBy(col("score").desc)

    df.withColumn("topR", rank().over(windowSpec))
      .withColumn("topDR", dense_rank().over(windowSpec))
      .withColumn("topN", row_number().over(windowSpec))
      .filter(col("topR").leq(3))
      .show()
  }
}

// Ref: https://blog.csdn.net/a934079371/article/details/109376370