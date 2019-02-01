name := "SentimentAnalysisModule"

val sparkVersion = "2.3.1"

version := "0.1"

scalaVersion := "2.11.8"
resolvers += "CakeSolutions" at "https://dl.bintray.com/cakesolutions/maven"
libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "2.1.0"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" classifier "models"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)
