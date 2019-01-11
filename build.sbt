name := "SentimentAnalysisModule"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "1.1.1"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" classifier "models"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.1"
libraryDependencies += "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3"
