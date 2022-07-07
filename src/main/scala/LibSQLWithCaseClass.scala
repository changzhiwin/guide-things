package zhiwin.spark.guide

import org.apache.spark.sql._

import org.apache.logging.log4j.scala.Logging

object LibSQLWithCaseClass extends Logging {

  case class Person(age: Int, name: String)

  def mapper(l: String): Person = {
    val fields = l.split(",")
    Person(fields(0).toInt, fields(1))
  }

  def demo(): Unit = {

    logger.trace("---> Enter demo()")

    val spark = SparkSession.builder().appName("Spark SQL").getOrCreate()

    val lines = spark.sparkContext.parallelize(Seq("23,Bob", "80,Bill", "43,Sam"))

    val people = lines.map(mapper)

    import spark.implicits._

    val schemaPeople = people.toDS()

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")

    val t = spark.sql("select * from people where age >= 13")

    val res = t.collect()

    res.foreach(r => logger.info(r))

    spark.stop()
    logger.trace("<--- Leave demo()")
  }

}