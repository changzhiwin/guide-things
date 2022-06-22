/Users/changzhi/Spark/spark-3.2.1-bin-hadoop3.2-scala2.13/bin/spark-submit \
--class zhiwin.spark.guide.MainApp \
--packages org.apache.logging.log4j:log4j-api:2.17.2 org.apache.logging.log4j:log4j-core:2.17.2 org.apache.logging.log4j:log4j-api-scala:12.0 \
--master "local[*]" \
target/scala-2.13/guide-things_2.13-1.0-SNAPSHOT.jar
