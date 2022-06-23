# How to run
```
sbt package

SPARK_HOME/spark-3.2.1-bin-hadoop3.2-scala2.13/bin/spark-submit \
--class zhiwin.spark.guide.MainApp \
--packages org.apache.logging.log4j:log4j-api:2.17.2,org.apache.logging.log4j:log4j-core:2.17.2 \
--jars ./jars/log4j-api-scala_2.13-12.0.jar \
--master "local[*]" \
target/scala-2.13/guide-things_2.13-1.0-SNAPSHOT.jar
```

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