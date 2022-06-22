package zhiwin.spark.guide

// ref: https://logging.apache.org/log4j/2.x/log4j-api/apidocs/index.html
// ref: https://index.scala-lang.org/apache/logging-log4j-scala
// ref: https://github.com/apache/logging-log4j-scala
// ref: https://logging.apache.org/log4j/scala/index.html

import org.apache.logging.log4j.scala.Logging

object MainApp extends Logging {

  def main(args: Array[String]) = {

    val sometime = "Sometime"
    
    logger.trace("Entering application.")
    logger.warn("I'm here.")
    logger.info(s"${sometime}, It's works.")
    logger.error("Did not do it.");

    SparkSQLWithCaseClass.demo()
  }
}