package zhiwin.spark.guide

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.scala.Logging

object LibHdfsReadAndWrite extends Logging {
  /**
  * Use local Pseudo-Distributed Mode
  * Make sure had install hadoop locally
  * Ref: https://hadoop.apache.org/docs/r3.2.3/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
  */
  def run(): Unit = {
    val spark = SparkSession.builder().appName("Spark HDFS").getOrCreate()

    val jsonDF = spark.read.json("hdfs://localhost:9000/user/foobar/*.json")

    jsonDF.printSchema()

    jsonDF.sample(0.1).repartition(3).write.format("csv").mode("overwrite").save("hdfs://localhost:9000/user/foobar/sample")

    spark.close()
  }
}