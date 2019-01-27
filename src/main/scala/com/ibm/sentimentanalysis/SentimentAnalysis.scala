package com.ibm.sentimentanalysis

import java.util
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object SentimentAnalysis {
  var slogger = Logger.getLogger(this.getClass.getName)

  //  case class TweetRecord(date: String, hashTag: String, sentiment: String, tweet: String) extends Serializable
  var counter:Int = 1
  def main(args: Array[String]): Unit = {

    val warehouseLocation = "/user/hive/warehouse"


    System.setProperty("spark.sql.warehouse.dir", warehouseLocation)
    System.setProperty("hive.metastore.uris", "thrift://127.0.0.1:9083/metastore")
    System.setProperty("spark.sql.catalogImplementation","hive")
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //val ssc:StreamingContext = sparks.streams

    val kafkaTopicRaw = "twitter_topic"
    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val rawStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    rawStream.foreachRDD { kafkaRdd =>
      println("kafka rdd : "+ kafkaRdd.count())
      val sparks = SparkSession.builder()
            .appName("Java Spark SQL basic example")
            .config("spark.master", "local[2]")
            .config("spark.sql.warehouse.dir", warehouseLocation)
            .config("hive.metastore.uris", "thrift://127.0.0.1:9083/metastore")
            .config("spark.sql.catalogImplementation","hive")
            .enableHiveSupport()
            .getOrCreate()

            import sparks.implicits._
            val myrdd = kafkaRdd.map (ele =>  ele._1.toString +","+ ele._2.substring(0, ele._2.indexOf("\t")).toString +","+
              SentimentAnalysisUtils.detectSentiment(ele._2.substring(ele._2.indexOf("\t"))).toString +","+ ele._2.substring(ele._2.indexOf("\t")).toString)//.saveAsTextFile("hdfs://localhost:8020/spark/input/").
            println("myrdd size "+myrdd.count())
            val df = myrdd.toDF()
            println("df : "+df.count())
      df.write.mode(SaveMode.Append).text("hdfs://localhost:8020/user/hive/warehouse/twitter/")
      counter = counter.toInt+df.count().toInt
      println("counter "+counter)
            //println("myrdd "+myrdd.count())


            //myrdd.saveAsTextFile("hdfs://localhost:8020/spark/input/")
              // QueryDF.main(Array(createdAt,hashTag,setiment,tweet))//insertToHive(createdAt,hashTag,setiment,tweet)
             // sqlco.sql("CREATE TABLE IF NOT EXISTS twitter(createdAt String, hashTag STRING, sentiment String, tweet String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
              //sqlco.sql("INSERT INTO table twitter values("+ele._1+","+ele._2.substring(0, ele._2.indexOf("\t")).toString+" ,"+SentimentAnalysisUtils.detectSentiment(ele._2.substring(ele._2.indexOf("\t")).toString).toString+", "+ele._2.substring(ele._2.indexOf("\t"))+")")
              //sqlco.sql("show tables").show()
              //sqlco.sql("select hashTag,sentiment from default.twitter").show()*/
        /*TweetRecord(ele._1,
          ele._2.substring(0, ele._2.indexOf("\t")).toString,
          SentimentAnalysisUtils.detectSentiment(ele._2.substring(ele._2.indexOf("\t")).toString).toString,
          ele._2.substring(ele._2.indexOf("\t")))*/
//      //}

      /*      println("WDC Count : "+wdc.count())
            if (wdc.count() > 0) {
              wdc.foreach{ tw => println("Get "+tw.get(1))}
              wdc.write.mode(SaveMode.Append).parquet("parquetfile.parquet")
            }*/
    }

     ssc.start()
     ssc.awaitTermination()
  }
}
