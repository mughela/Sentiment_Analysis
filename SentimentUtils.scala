package com.datastacks.sparktwitter
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
object SentimentUtils {
  val nlpPropts = {
    val propts = new Properties()
    propts.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    propts
  }
def detectSentiment(message: String): String = {
    val pipeline = new StanfordCoreNLP(nlpPropts)
    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()
    var longest = 0
    var mainSentiment = 0
    for (tweetMsg <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val parseTree = tweetMsg.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val tweetSentiment = RNNCoreAnnotations.getPredictedClass(parseTree)
      val partText = tweetMsg.toString
      if (partText.length() > longest) {
        mainSentiment = tweetSentiment
        longest = partText.length()
      }

      sentiments += tweetSentiment.toDouble
      sizes += partText.length
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))

    if (weightedSentiment <= 0.0)
      "NOT_UNDERSTOOD"
    else if (weightedSentiment < 1.6)
      "NEGATIVE"
    else if (weightedSentiment <= 2.0)
      "NEUTRAL"
    else if (weightedSentiment < 5.0)
      "POSITIVE"
    else "NOT_UNDERSTOOD"    
  }
}
  