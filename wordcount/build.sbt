
// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.15"

name := "word-count"
version := "1.0"

val sparkVersion = "3.1.2"

mainClass in Compile := Some("es.upm.dit.WordCountBatchJob")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
