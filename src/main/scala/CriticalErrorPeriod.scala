package zhiwin.spark.guide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.apache.logging.log4j.scala.Logging

/**
  * 机器的错误日志，分为一般错误、致命错误，需要找出每次致命错误之前，与每种一般错误的时间间隔（单位：天）
  * 数据格式：机器名、错误时间、错误类型
  * 关键点：每种一般错误需要独立的列来保存，否则混在一起的话，在window中取不到对应的数据
  */

object CriticalErrorPeriod extends Logging {

  case class ErrorLog(name: String, date: String, kind: String)

  def run(): Unit = {
    val spark = SparkSession.builder().appName("Critical Error Period").getOrCreate()
    // val sc = spark.sparkContext

    val data = Seq(
      ErrorLog("m1", "2021-01-01", "e1"), 
      ErrorLog("m1", "2021-02-02", "e1"),
      ErrorLog("m1", "2021-03-03", "e2"), 
      ErrorLog("m1", "2021-03-04", "critical"), 
      ErrorLog("m1", "2021-07-05", "e1"), 
      ErrorLog("m2", "2021-01-01", "e1"), 
      ErrorLog("m2", "2021-06-02", "critical"), 
      ErrorLog("m2", "2021-08-03", "e2"), 
      ErrorLog("m2", "2021-09-04", "e1"), 
      ErrorLog("m2", "2021-10-17", "critical"),
      ErrorLog("m2", "2021-10-25", "e1"),
    )

    var df = spark.createDataFrame(data)

    // df.printSchema()

    val errors = Seq("e1", "e2")
    errors.foreach { err =>
      df = df.withColumn(s"${err}_occur_date", when(col("kind") === err, col("date")).otherwise(null))
    }

    //df.printSchema()

    val windowSpec = Window.partitionBy(col("name")).orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    errors.foreach { err =>
      df = df.withColumn(s"${err}_occur_last", last(col(s"${err}_occur_date"), true).over(windowSpec))
             .withColumn(s"${err}_period_days", datediff(col("date"), col(s"${err}_occur_last")))
    }

    df.filter(col("kind").equalTo("critical")).show()

  }
}

// Ref: https://blog.damavis.com/en/the-use-of-window-in-apache-spark/
    
/*
+----+----------+--------+-------------+-------------+-------------+--------------+-------------+--------------+
|name|      date|    kind|e1_occur_date|e2_occur_date|e1_occur_last|e1_period_days|e2_occur_last|e2_period_days|
+----+----------+--------+-------------+-------------+-------------+--------------+-------------+--------------+
|  m1|2021-03-04|critical|         null|         null|   2021-02-02|            30|   2021-03-03|             1|
|  m2|2021-06-02|critical|         null|         null|   2021-01-01|           152|         null|          null|
|  m2|2021-10-17|critical|         null|         null|   2021-09-04|            43|   2021-08-03|            75|
+----+----------+--------+-------------+-------------+-------------+--------------+-------------+--------------+
*/