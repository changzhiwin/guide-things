package zhiwin.spark.guide

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{ GroupStateTimeout, OutputMode, GroupState }
import org.apache.spark.sql.streaming.{ StreamingQueryListener }

import org.apache.logging.log4j.scala.Logging

object LibStreaming extends Logging {

  case class InputRow(uid: String, timestamp: java.sql.Timestamp, x: Double, activity: String)

  case class UserSession(val uid: String, var timestamp: java.sql.Timestamp, 
      var activities: Array[String], var values: Array[Double]
  )

  case class UserSessionOutput(val uid: String, var activities: Array[String], var xAvg: Double)

  def run(): Unit = {
    val spark = SparkSession.builder().appName("Spark Streaming").getOrCreate()

    // 200 is too big for demo
    spark.conf.set("spark.sql.shuffle.partitions", 5)

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("---> Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("---> Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: StreamingQueryListener.QueryProgressEvent): Unit = {
        println("---> Query made progress: " + queryProgress.progress.id)
      }
    })

    import spark.implicits._
    val streaming = spark.readStream.
      schema("Arrival_Time LONG, Creation_Time LONG, Device STRING, Index LONG, Model STRING, User STRING, gt STRING, x DOUBLE, y DOUBLE, z DOUBLE").
      option("maxFilesPerTrigger", 5).
      json("./data/activity-data")

    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time"
    )

    val sessionStream = withEventTime.where("x is not null").
      selectExpr("user as uid", "x", "gt as activity",
        "cast(cast(Creation_Time as double)/1000000000 as timestamp) as timestamp"
      ).
      as[InputRow].
      withWatermark("timestamp", "5 seconds").
      groupByKey(_.uid).
      flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.EventTimeTimeout)(updateAcrossEvents).
      writeStream.
      queryName("count_based_device").
      format("memory").
      start()

    var count = 20
    while (sessionStream.isActive && count > 0) {
      Thread.sleep(5000)
      spark.sql("SELECT count(*) FROM count_based_device").show(false)
      count -= 1
    }

    sessionStream.stop()
  }

  def updateWithEvent(state: UserSession, input: InputRow): UserSession = {
    if (!state.activities.contains(input.activity)) {
      state.activities = state.activities ++ Array(input.activity)
    }
    
    state.values = state.values ++ Array(input.x)

    if (input.timestamp.after(state.timestamp)) {
      state.timestamp = input.timestamp
    }

    state
  }

  def updateAcrossEvents(uid: String, inputs: Iterator[InputRow], oldState: GroupState[UserSession]): Iterator[UserSessionOutput] = {

    // Notice: iterator is mutable
    val inputSeq = inputs.toSeq
    logger.info(s"updateAcrossEvents uid = ${uid} input len = ${inputSeq.size}")

    var stateValue = if (!oldState.exists) {
      UserSession(uid, new java.sql.Timestamp(0), Array.empty, Array.empty)
    } else {
      oldState.get
    }

    stateValue = inputSeq.foldLeft(stateValue)( (st, input) => updateWithEvent(st, input))

    if (oldState.hasTimedOut) {
      oldState.remove()
      logger.info(s"updateAcrossEvents timeout [${uid}]")
      formOutputs(stateValue)
    } else {
      if (!stateValue.timestamp.equals(new java.sql.Timestamp(0))) {
        oldState.update(stateValue)
        // update timeout also
        logger.info(s"updateAcrossEvents update [${uid}] at ${stateValue.timestamp.getTime()}")
        oldState.setTimeoutTimestamp(stateValue.timestamp.getTime(), "5 seconds")
      }

      Iterator.empty
    }
  }

  private def formOutputs(session: UserSession): Iterator[UserSessionOutput] = {
    if (session.values.isEmpty) {
      Iterator.empty
    } else {
      Iterator(UserSessionOutput(session.uid, session.activities, session.values.sum/session.values.length.toDouble))
    }
  }
}