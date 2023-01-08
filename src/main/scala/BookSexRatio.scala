package zhiwin.spark.guide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.logging.log4j.scala.Logging

/**
  * 有hdfs文件，文件每行的格式为作品ID，用户id，用户性别。
  * 请用一个spark任务实现以下功能：统计每个作品对应的用户（去重后）的性别分布。
  * 输出格式如下：作品ID，男性用户数量，女性用户数量
  */

object BookSexRatio extends Logging {

  def run(): Unit = {
    val spark = SparkSession.builder().appName("Sex Ratio of Book").getOrCreate()

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("hdfs://192.168.31.130:9000/spark/demo/books.csv")

    df.printSchema()

    df
      .groupBy(col("book"), col("sex"))
      .agg(countDistinct(col("userId")))
      .orderBy(col("book"), col("sex"))
      .show()

    // TODO，比率
  }
}

// Ref: https://www.cnblogs.com/exmyth/p/16530460.html