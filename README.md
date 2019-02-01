    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val topics = "twitter_topicv6"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test-consumer-group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1"
    )
    val recordsStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    recordsStream.foreachRDD



import org.apache.kafka.clients.consumer
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
