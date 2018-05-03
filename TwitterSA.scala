package com.datastacks.sparktwitter
import com.datastacks.sparktwitter.SentimentUtils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import twitter4j.Status
import java.util.Date
import java.text.SimpleDateFormat
import org.joda.time.{DateTime,Days}
import java.util.Locale
import org.json4s.JsonDSL
import org.elasticsearch.spark.rdd.EsSpark
import java.io.File
import scala.util.Try

object TwitterSA {
  def main(args: Array[String]) {

     if (args.length < 4) {
       System.err.println("Usage: TwitterSentimentAnalysis <consumer key> <consumer secret> " +
         "<access token> <access token secret> [<filters>]")
       System.exit(1)
     }

     val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
     val filters = args.takeRight(args.length - 4)

     System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
     System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
     System.setProperty("twitter4j.oauth.accessToken", accessToken)
     System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

     val conf = new SparkConf().setAppName("Msenti Fanal").setMaster("local[4]")
     conf.set("es.resource","twitter_020717/tweet")
     conf.set("es.index.auto.create","false")
     conf.set("es.nodes","localhost")
     conf.set("es.port","9200")
     Streaminglog.setStreamingLogLevels()
     val ssc = new StreamingContext(conf, Seconds(5))
     val tweets = TwitterUtils.createStream(ssc, None, filters)
     tweets.print()
     tweets.foreachRDD{(rdd, time) =>
       rdd.map(t => {
         Map(
           "user"-> t.getUser.getScreenName,
           "created_at" -> t.getCreatedAt.getTime.toString,
           "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
           "text" -> t.getText,
           "hashtags" -> t.getHashtagEntities.map(_.getText),
           "retweet" -> t.getRetweetCount,
           "sentiment" -> detectSentiment(t.getText).toString
         )
       }).saveToEs("twitter_020717/tweet")
     }


     ssc.start()
     ssc.awaitTermination()
 }
}