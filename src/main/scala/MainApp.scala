package zhiwin.spark.guide

// ref: https://logging.apache.org/log4j/2.x/log4j-api/apidocs/index.html
// ref: https://index.scala-lang.org/apache/logging-log4j-scala
// ref: https://github.com/apache/logging-log4j-scala
// ref: https://logging.apache.org/log4j/scala/index.html

import org.apache.logging.log4j.scala.Logging

object MainApp extends Logging {

  def main(args: Array[String]) = {
    
    logger.trace("Entering application.")

    SparkSQLWithCaseClass.demo()
    
  }
}