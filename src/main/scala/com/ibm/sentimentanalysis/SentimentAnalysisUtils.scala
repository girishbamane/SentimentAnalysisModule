package com.ibm.sentimentanalysis

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object SentimentAnalysisUtils {
  val prop = {
    val properties = new Properties()
    properties.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    properties
  }

  def detectSentiment(message: String): SENTIMENT_TYPE = {
    Logger.getRootLogger.setLevel(Level.INFO)
    val pipeline = new StanfordCoreNLP(prop)
    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()
    var longest = 0
    var mainSentiment = 0
    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val strSentence = sentence.toString
      if (strSentence.length() > longest) {
        mainSentiment = sentiment
        longest = strSentence.length()
      }
      sentiments += sentiment.toDouble
      sizes += strSentence.length
    }
    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))
    if (sentiments.size == 0) {
      mainSentiment = -1
      weightedSentiment = -1
    }
    weightedSentiment match {
      case s if s <= 0.0 => NOT_UNDERSTOOD
      case s if s < 1.0 => VERY_NEGATIVE
      case s if s < 2.0 => NEGATIVE
      case s if s < 3.0 => NEUTRAL
      case s if s < 4.0 => POSITIVE
      case s if s < 5.0 => VERY_POSITIVE
      case s if s > 5.0 => NOT_UNDERSTOOD
    }

  }

  trait SENTIMENT_TYPE

  case object VERY_NEGATIVE extends SENTIMENT_TYPE

  case object NEGATIVE extends SENTIMENT_TYPE

  case object NEUTRAL extends SENTIMENT_TYPE

  case object POSITIVE extends SENTIMENT_TYPE

  case object VERY_POSITIVE extends SENTIMENT_TYPE

  case object NOT_UNDERSTOOD extends SENTIMENT_TYPE

}