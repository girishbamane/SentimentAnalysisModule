package com.ibm.sentimentanalysis

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object SentimentAnalysis {
  var slogger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val TOPIC = "twitter_topic"
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")  // Ask Sanjay
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "test-consumer-group")     // Ask Sanjay
    val conf = new SparkConf(true)
      .setAppName("SentimentAnalysis")
      .set("spark.cassandra.connection.host", "localhost")   // Get it from env
    val ssc = new SparkContext(conf);
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList(TOPIC))
    while (true) {
      val records = consumer.poll(2)
      for (record <- records.asScala) {
        var createdAt = record.key()
        var tweetData = record.value()
        var hashTag = ""
        var sentiment = ""
        var tweet = tweetData.substring(tweetData.indexOf("\t")).replaceAll("[\\.$|,|;|']", "").trim
        if (tweetData.contains("\t")) {
          hashTag = tweetData.substring(0, tweetData.indexOf("\t"))
          sentiment = SentimentAnalysisUtils.detectSentiment(tweet).toString
          CassDBUtil.saveDataToCassandra(ssc, createdAt, hashTag, sentiment, tweet)
          slogger.info(s"CreatedAt : $createdAt HashTag : $hashTag Tweet : $tweet Sentiment : $sentiment ")
        }
      }
    }
  }
}