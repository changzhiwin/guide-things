package zhiwin.spark.guide

case class Person(frist: String, last: String)

class DatasetExampleTest extends BaseSpec {

  // must have this, for toDF/toDS
  import TestImplicits._

  "DataFrame formatted strings" should "just work" in {
    val authors = Seq("bill,databricks", "matei,databricks")

    val authorsRDD = spark.
      sparkContext.
      parallelize(authors).
      toDF()
    
    assert(authorsRDD.count() == 2)
  }

  "Dataset filter string" should "just work" in {
    val authors = Seq("bill,databricks", "matei,databricks")

    val authorsDataset = spark.
      sparkContext.
      parallelize(authors).
      map(au => Person(au.split(",")(0), au.split(",")(1))).
      toDS()

    authorsDataset.show()
    assert(authorsDataset.filter(p => p.frist.contains("ll")).count() == 1)
  }
}