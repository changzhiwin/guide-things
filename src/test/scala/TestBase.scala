package zhiwin.spark.guide

import org.scalatest._
import flatspec.AnyFlatSpec
import org.scalatest.matchers.should._
import org.apache.spark.sql.{ SparkSession, SQLImplicits, SQLContext }

// ref: https://www.scalatest.org/scaladoc/3.2.11/org/scalatest/BeforeAndAfterEach.html
// ref: https://www.scala-sbt.org/1.x/docs/Testing.html
// ref: https://docs.scala-lang.org/getting-started/sbt-track/testing-scala-with-sbt-on-the-command-line.html

abstract class BaseSpec extends AnyFlatSpec with BeforeAndAfterEach with Matchers {
  var spark: SparkSession = _

  // required, for implicits
  object TestImplicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    spark = SparkSession.
      builder().
      appName("Testing").
      master("local").
      config("spark.driver.allowMultipleContexts", "false").
      getOrCreate()
  }

  override def afterEach() = {
    try {
      super.afterEach()
    }
    finally {
      spark.stop()
    }

  }
}