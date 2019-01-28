package com.ibm.sentimentanalysis

import kafka.serializer.StringDecoder
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SentimentAnalysisNew {

  //var slogger = Logger.getLogger(this.getClass.getName)

  case class TweetRecord(dates: String, hashTag: String, sentiment: String, tweet: String) extends Serializable

  var counter: Int = 1

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "/user/hive/warehouse"
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
      .set("spark.sql.warehouse.dir", warehouseLocation)
      .set("hive.metastore.uris", "thrift://127.0.0.1:9083/metastore")
      .set("spark.sql.catalogImplementation", "hive")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kafkaTopicRaw = "twitter_topic"
    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val rawStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    rawStream.foreachRDD { kafkaRdd =>
      val sparks: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
      import sparks.implicits._
      val df = kafkaRdd.map(ele => TweetRecord(ele._1.toString, ele._2.substring(0, ele._2.indexOf("\t")).toString,
        SentimentAnalysisUtils.detectSentiment(ele._2.substring(ele._2.indexOf("\t"))).toString,
        ele._2.substring(ele._2.indexOf("\t")).toString)).toDF("dates", "hashtag", "sentiment", "tweet")
      if (df.count() > 0) {
        println("MyRddCount : " + df.count())
        df.write.mode(SaveMode.Append).option("spark.sql.parquet.compression.codec","snappy")saveAsTable("twitterdb.twitterparq")
        counter = counter.toInt + df.count().toInt
        println("counter " + counter)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
