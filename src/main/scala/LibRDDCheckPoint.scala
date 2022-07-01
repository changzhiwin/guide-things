package zhiwin.spark.guide

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.scala.Logging

object LibRDDCheckPoint extends Logging {

  def run(): Unit = {

    logger.trace("-------------> running...")
    logger.warn("-------------> Warn running...")

    val spark = SparkSession.builder().appName("Spark RDD CheckPoint").getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoint0701")

    val srcData = Seq[(Int, Char)]((1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (3, 'f'), (2, 'g'), (1, 'h'))
    val inputRDD = sc.parallelize(srcData, 3)

    val mappedRDD = inputRDD.map(r => (r._1 + 10, r._2.toString))
    mappedRDD.cache()

    val reducedRDD = mappedRDD.reduceByKey((x, y) => s"${x}_${y}", 2)
    reducedRDD.cache()
    reducedRDD.checkpoint()

    val groupedRDD = mappedRDD.groupByKey().mapValues(v => v.toList)
    groupedRDD.cache()
    groupedRDD.checkpoint()

    groupedRDD.foreach(println)

    val joinedRDD = reducedRDD.join(groupedRDD)

    joinedRDD.foreach(println)
  }

  def runDF(): Unit = {
    val spark = SparkSession.builder().appName("Spark DataFrame DAG").getOrCreate()

    val srcData = Seq[(Int, Char)]((1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (3, 'f'), (2, 'g'), (1, 'h'))
    val inputRDD = sc.parallelize(srcData, 3)
    val mappedRDD = inputRDD.map(r => (r._1 + 10, r._2.toString))

    // Notice: have 2 jobs, some different from RDD
    mappedRDD.toDF().groupBy("_1").agg(collect_list("_2")).show(false)
  }

}