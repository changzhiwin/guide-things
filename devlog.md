# Problem Fix
## 1, Log lib different between driver and spark framework
Spark log config at `SPARK_HOME/etc/log4j.properties`. Driver log shoud do it by youself, like `log4j2.xml`.

## 2. In test env, `toDF/toDS` not work, even `import spark.implicits._`
```
  // define in the TestBase
  object TestImplicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  // then import in the TeseCase
  import TestImplicits._
```

## 3. Spark 3.3 use log4j2, and cover Driver log level
```
// spark-3.2.1, log4j.rootCategory=WARN, console
22/07/01 16:52:22 INFO MainApp$: ------> Info, Entering application.
22/07/01 16:52:22 ERROR MainApp$: ------> Error, Entering application.
22/07/01 16:52:22 TRACE LibRDDCheckPoint$: -------------> running...
22/07/01 16:52:22 WARN LibRDDCheckPoint$: -------------> Warn running...

// spark-3.3.0, rootLogger.level = warn
22/07/01 16:50:31 ERROR MainApp$: ------> Error, Entering application.
22/07/01 16:50:31 WARN LibRDDCheckPoint$: -------------> Warn running...
```

## 4. An iterator is mutable
```
// iterator.length  // can't use !!!
logger.warn(s"updateAcrossEvents iterator.length = ${inputs.length}")
logger.warn(s"updateAcrossEvents iterator.hasNext = ${inputs.hasNext}")

// 22/07/06 17:09:08 WARN LibStreaming$: updateAcrossEvents iterator.length = 45620
// 22/07/06 17:09:08 WARN LibStreaming$: updateAcrossEvents iterator.hasNext = false
```