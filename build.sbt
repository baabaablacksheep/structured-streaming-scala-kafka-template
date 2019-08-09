name := "Location_Estimation_Streaming_Test_2"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.2"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,

  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)
