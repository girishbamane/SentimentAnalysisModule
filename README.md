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



start zookeeper - 

bin\windows\zookeeper-server-start.bat config\zookeeper.properties

start kafka server - 

bin\windows\kafka-server-start.bat .\config\server.properties

1. Create Topic 

bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter_topicv3

	Created - twitter_topicv3

2. List topics 

bin\windows\kafka-topics.bat --list --zookeeper localhost:2181

3. Start Producer 

bin\windows\kafka-console-producer.bat 

4. Start Consumer 

bin\windows\kafka-console-consumer.bat 	--bootstrap-server localhost:9092 --topic twitter_topicv3 --from-beginning
10604

  


bin\windows\kafka-run-class.bat kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic twitter_topicv3

\bin\windows\kafka-topics.bat --zookeeper localhost:2181 --alter --topic twitter_topicv3 --config retention.ms=1

Delete topic in kafka 

Stop Kafka server
Delete the topic directory with rm -rf command
Connect to Zookeeper instance: zookeeper-shell.sh host:port
ls /brokers/topics
Remove the topic folder from ZooKeeper using rmr /brokers/topics/yourtopic
Restart Kafka server
Confirm if it was deleted or not by using this command kafka-topics.sh --list --zookeeper host:port
