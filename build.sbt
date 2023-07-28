name := "kafkastream_iceberg"
version := "1.0"
organization := "com.example"
scalaVersion := "2.12.10"

//libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.1"
//libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.3.0" 
//libraryDependencies += "org.projectnessie" % "nessie-spark-3.2-extensions" % "0.43.0" 
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1" % "provided"


lazy val app = (project in file("."))
  .settings(
    assembly / mainClass := Some("kafkastream_spark_iceberg_rest")
    // more settings here ...
  )

