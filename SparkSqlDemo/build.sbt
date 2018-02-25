name := "SparkSqlDemo"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.11.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.0"