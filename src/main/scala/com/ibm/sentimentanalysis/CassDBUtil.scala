package com.ibm.sentimentanalysis

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark._

object CassDBUtil extends App {
  def saveDataToCassandra(ssc: SparkContext, createdAt: String, hashTag: String, sentiment: String, tweet: String): Boolean = {
    val cassInsert = CassandraConnector.apply(ssc.getConf)
    cassInsert.withSessionDo(session =>
      session.execute("insert into sentimentdb.sentiment (createdat, hashtag, sentiment, tweet) values ('" + createdAt + "','" + hashTag + "','" + sentiment + "','" + tweet + "')").isExhausted)
  }
}