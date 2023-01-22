package zhiwin.spark.guide

// ref: https://logging.apache.org/log4j/2.x/log4j-api/apidocs/index.html
// ref: https://index.scala-lang.org/apache/logging-log4j-scala
// ref: https://github.com/apache/logging-log4j-scala
// ref: https://logging.apache.org/log4j/scala/index.html

import org.apache.logging.log4j.scala.Logging

object MainApp extends Logging {

  def main(args: Array[String]) = {

    // because Spark config set Log Level = warn
    logger.warn("---> Entering application.")
    
    val command = if (args.length == 0) "RDD" else args(0)

    command.toUpperCase match {
      case "JOIN"   => SortMergeJoin.run()
      case "ERROR"  => CriticalErrorPeriod.run()
      case "TOPN"   => EachKeyTopN.run()
      case "LOGIN"  => ContinueLogin.run()
      case "BOOK"   => BookSexRatio.run()
      case "NUM"    => FindNumber.run()
      case "SQL"    => LibSQLWithCaseClass.demo()
      case "STREAM" => LibStreaming.run()
      case "HDFS"   => LibHdfsReadAndWrite.run()
      case _        => LibRDDCheckPoint.run()
    }

  }
}