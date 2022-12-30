package zhiwin.spark.guide

/**
  * 场景：大約20000亿的数字，存储HDFS上，这些数字都是分布0~20000亿之间，各不相同，会有缺失的部分，需要基于Spark算缺失的部分
  * 要求：用java或scala编写 需要考虑单个节点的资源和性能问题 （4G内存)
  * 实现：RDD控制partition，BitMap过滤，示例实现只用了100的数
  * 20000亿：分成10000个分区，每个分区2亿个数
  * 占用内存：8 * 2亿 字节 = 1.5G左右，加上BitMap需要 2亿 / 8 字节 = 25M
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner

import org.apache.logging.log4j.scala.Logging

object FindNumber extends Logging {

  object ModPartitioner extends Partitioner {

    override def getPartition(key: Any): Int = (key.asInstanceOf[Long] / 10).toInt

    override def numPartitions = 10
  }

  def run(): Unit = {
    val spark = SparkSession.builder().appName("Find no present Numbers").getOrCreate()
    val sc = spark.sparkContext

    val numbers = Seq[Long](
      60,61,62,63,64,65,66,67,68,79,
      70,71,72,73,74,75,76,78,69,       // miss 77
      80,81,82,83,84,85,86,87,88,99,
      90,91,92,93,94,95,96,97,98,89,
      0,1,2,3,4,5,6,7,9,                 // miss 8
      10,11,12,13,14,15,16,17,19,18,
      20,21,22,23,24,25,26,27,29,28,
      30,31,32,33,34,35,36,37,39,38,
      40,42,43,44,45,46,47,49,48,        // miss 41
      50,51,52,53,54,56,57,58,59,        // miss 55
    )
    val numRDD = sc.parallelize(numbers, 2)

    val kvRDD = numRDD.map(n => (n, true)).partitionBy(ModPartitioner)

    val partRDD = kvRDD.mapPartitionsWithIndex { (idx, it) =>

      val start = idx * 10   // include
      val end   = start + 10 // not include
      
      val bf = new BitMapLive(9) // 0 - 9
      it.foreach { x =>
        bf.add( (x._1 - start).toInt )
      }

      val notFounds = (start until end).flatMap { n =>
        bf.contains(n - start) match {
          case false => Seq(n)
          case true  => Nil
        }
      }
      notFounds.iterator
    }

    partRDD.collect().foreach(item => println(s"value = ${item}"))

    spark.close()
  }
}