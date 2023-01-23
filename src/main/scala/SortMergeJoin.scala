package zhiwin.spark.guide

import scala.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import org.apache.logging.log4j.scala.Logging

case class User(uid: Int, login: String, email: String, userState: String)
case class Order(transactionId: Int, quantity: Int, userId: Int, amount: Double, state: String, items: String)

object SortMergeJoin extends Logging {

  val states = Map(0 -> "AZ", 1 -> "CO", 2 -> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")

  val items = Map(0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")

  def run(): Unit = {
    val spark = SparkSession.builder().appName("Sort Merge Join Demo").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    import spark.implicits._
    val usersDF = (0 to 100).map(id => User(id, s"user_${id}", s"user_${id}@dod.com", states(Random.nextInt(6)))).toDF()

    // usersDF.show(3)

    usersDF.orderBy(asc("uid")).write.format("json").bucketBy(4, "uid").mode(SaveMode.Overwrite).saveAsTable("UsersTable")

    val ordersDF = (0 to 100).map(r => Order(r, r, Random.nextInt(100), 10 * r * 0.2d, states(Random.nextInt(6)), items(Random.nextInt(6)))).toDF()

    // ordersDF.show(3)

    ordersDF.orderBy(asc("userId")).write.format("json").bucketBy(4, "userId").mode(SaveMode.Overwrite).saveAsTable("OrdersTable")

    // val userOrderDF = ordersDF.join(usersDF, usersDF.col("uid") === ordersDF.col("userId")).show(10)

    spark.sql("CACHE TABLE UsersTable")
    spark.sql("CACHE TABLE OrdersTable")

    val usersBucketDF = spark.table("UsersTable") 
    val ordersBucketDF = spark.table("OrdersTable")
    //println(s"user partitions = ${usersBucketDF.rdd.getNumPartitions}, orders partitions = ${ordersBucketDF.rdd.getNumPartitions}")

    // 验证单个分区里的key，所属集合是一致的
    usersBucketDF.select("uid").mapPartitions(iter => Seq( Set.from(iter.map(_.getInt(0))).toSeq.sorted.mkString(",") ).iterator).foreach(s => println(s))
    println("--------\n")
    ordersBucketDF.select("userId").mapPartitions(iter => Seq( Set.from(iter.map(_.getInt(0))).toSeq.sorted.mkString(",") ).iterator  ).foreach(s => println(s))

    //val joinUsersOrdersBucketDF = ordersBucketDF.join(usersBucketDF, col("userId") === col("uid"))
    //println(s"joinUsersOrdersBucketDF partition = ${joinUsersOrdersBucketDF.rdd.getNumPartitions}")
    //joinUsersOrdersBucketDF.show(10)
  }
}