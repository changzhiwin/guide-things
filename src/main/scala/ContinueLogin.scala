package zhiwin.spark.guide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.apache.logging.log4j.scala.Logging

/**
  * 例如计算平台连续登陆3天以上的用户统计
  * userId, loginTime
  */

object ContinueLogin extends Logging {

  def run(): Unit = {
    val spark = SparkSession.builder().appName("Continue Login 3 days").getOrCreate()
    
    val df = spark
      .read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load("./data/userlog.csv")

    df.printSchema()

    val windowSpec = Window.partitionBy("userId").orderBy("loginDate")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)       // have orderBy, this the default

    // 关键点：使用row_number()，取得相对值，让多行状态数据变成单行数据
    df.withColumn("loginDate", to_date(col("loginTime")))
      //.withColumn("pastDays", datediff(current_date(), col("loginDate")))
      //.filter(col("pastDays").lt(8))
      .filter(datediff(current_date(), col("loginDate")).lt(8))
      .orderBy(col("userId"))
      .withColumn("order", row_number().over(windowSpec))
      .withColumn("diffDate", date_sub(col("loginDate"), col("order")))
      .groupBy(col("userId"), col("diffDate"))
      .agg(
        count(col("userId")).as("countContinue"),  
        min(col("loginDate")).as("earlyLogin"),
        max(col("loginDate")).as("lastLogin")
      )
      .filter(col("countContinue").geq(3))
      .show()
  }
}

// Ref: https://zhuanlan.zhihu.com/p/440179932