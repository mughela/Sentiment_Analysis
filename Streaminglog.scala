package com.datastacks.sparktwitter
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.Logging
object Streaminglog extends Logging {
  def setStreamingLogLevels() {
    val log4jInitialised = Logger.getRootLogger.getAllAppenders.hasMoreElements()
    if(!log4jInitialised) {
      logInfo("Setting log level to [WARN] for streaming example" + "To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}