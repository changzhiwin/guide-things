package zhiwin.spark.guide

import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner

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
    df.show()
  }
}

// Ref: https://zhuanlan.zhihu.com/p/440179932