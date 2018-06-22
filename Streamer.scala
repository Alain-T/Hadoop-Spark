package dsti

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.storage.StorageLevel

import twitter4j.Status
import java.util.Calendar
import java.text.SimpleDateFormat

/**
 * @author ${user.name}
 */
object Streamer {
  /** Clears a area in a string (replacing characters by spaces),
    * between a start and an end position.
    *
    * @param str the string to be cleared
    * @param start the index in the string of the first character to clear
    * @param end the index in the string of the last character to clear
    */
  def clearText(str : Array[Char], start : Int, end : Int): Unit =
  {
    for(ind <- start until end) {
      str(ind) = ' '
    }
  }

  /** @return the tweet core text, i.e. the plain words it contains
    *         (i.e. ignoring media entities, URLs and user mention are removed)
    */
  def getCoreText(status : Status): String =
  {
    // create a mutable String
    val text = status.getText.toArray

    // clear all media entities
    for (mediaEntity <- status.getMediaEntities)
    {
      clearText(text, mediaEntity.getStart, mediaEntity.getEnd)
    }

    // clear all URLs
    for (urlEntities <- status.getURLEntities)
    {
      clearText(text, urlEntities.getStart, urlEntities.getEnd)
    }

    // clear all user mention (i.e. @)
    for (userMentionEntities <- status.getUserMentionEntities)
    {
      clearText(text, userMentionEntities.getStart, userMentionEntities.getEnd)
    }

    // pack all duplicate spaces
    "\\s+".r.replaceAllIn(text, " ")
  }

  /** Retrieve tweets from Twitter, filtering them per keywords and language.
    *
    * The number of tweets to retrieve corresponds to a minimum value,
    * in order for the pipeline to stop.
    *
    * @param nbTweets the number of tweets to retrieved
    * @param batchDuration the time interval at which streamed tweets will be
    *                      divided into batches
    * @param language the tweet language as a BCP 47 language indentifier.
    *                 (i.e. a 2 letter character code, e.g."en", "fr")
    * @param keywords the keywords to match tweets on
    */
  def retrieveTweets(nbTweets : Int, batchDuration : Duration,
                     language: String, keywords : Array[String])
  {
    // some utility function definition to manipulate tweet text

    // convert non Latin-1 characters and comma to spaces
    // note : comma is cleared to allow using CSV format
    val toPlainChar = (s: String) => s.replaceAll("[^ -+--\u00FF]", " ")

    // Latin-1 character set definition :
    // https://www.fileformat.info/info/charset/ISO-8859-1/list.htm

    // only keeps words (including accentued characters) and numbers:
    // all other characters are turned to space
    val toWordAndNum = (s: String) => s.replaceAll("[!-/:-@\\[-`{-\u00BF\u00D7\u00F7]", " ")

    // set the name of the application and the number of jobs a container can
    // simultaneously process (spark.executor.cores property).
    // Requires yarn.scheduler.maximum-allocation-vcores >= 2;
    // set from Ambari or directly in /etc/hadoop/conf/yarn-site.xml
    val config = new SparkConf().setAppName("twitter-stream-at")
      .set("spark.executor.cores", "2")

    // create the SparkContext of the application
    val sparkContext = new SparkContext(config)

    // print logs of level WARNING or higher
    sparkContext.setLogLevel("WARN")

    // counter for the total number of tweets retrieved
    var nbTweetsRetrieved = 0 : Long

    // create the StreamingContext from which tweets will be retrieved
    val streamingContext = new StreamingContext(sparkContext, batchDuration)

    // request tweets matching the requested keywords
    val tweets = TwitterUtils.createStream(streamingContext, None, keywords,
      StorageLevel.MEMORY_ONLY_SER)

    // only keep the tweets that matched the requested language
    val filteredTweets: DStream[Status] = tweets.filter(_.getLang == language)

    // iterate over all returned Discretized Stream
    filteredTweets.foreachRDD( foreachFunc = (rdd, time) => {
      // get the number of tweets
      val rddCount = rdd.count()

      // group tweets par 1000 (for efficiency)
      val nbPartitions = math.ceil(rddCount.toDouble / 1000).toInt

      // if the rdd is empty, drop it (to avoid empty rdd to be stored in HDFS)
      if (rddCount > 0) {
        // create the filename where tweets will be stored
        val filePath = "/user/alain-T/tweets/time=%s".format(time.milliseconds)
        println("%s : %s".format(filePath, rddCount))

        // extract tweet information, formating them in CSV
        val csvTweets = rdd.map(status => {
          // get the core text of the tweet, i.e. only the plain words in lower case
          val coreText = getCoreText(status)
          val plainText = toPlainChar(coreText).toLowerCase
          val compactText = "\\s+".r.replaceAllIn(toWordAndNum(plainText), " ").trim

          // the tweet information that will be saved
          (
            status.getId,
            compactText,
            status.getRetweetCount,
            status.getMediaEntities.length,
            status.getURLEntities.length,
            status.getUserMentionEntities.length,
            status.getUser.getId,
            toPlainChar(status.getUser.getScreenName),
            status.getUser.getFollowersCount,
            status.getUser.getFriendsCount
          )
        }).map(_.productIterator.mkString(","))

        // group the tweets and save them
        csvTweets.repartition(nbPartitions).saveAsTextFile(filePath)

        // compute the updated number of tweet retrieved
        nbTweetsRetrieved += rddCount
      }
      else {
        // empty rdd
        println("No tweet retrieved")
      }
    })

    // start the pipeline (i.e. Streamin
    streamingContext.start()

    val timeFormat = new SimpleDateFormat("HH:mm:ss")

    var isStopRequested = false
    var isStreamContextStopped = false

    while(! isStreamContextStopped)
    {
      // wait for termination of the streaming context or timeout after 1000 milliseconds
      isStreamContextStopped = streamingContext.awaitTerminationOrTimeout(1000)

      val now = timeFormat.format(Calendar.getInstance().getTime())
      println(
        "%s: awaitTerminationOrTimeout:isTerminated=%s, nbTweetsRetrieved=%s, isStopRequested=%s".
          format(now, isStreamContextStopped, nbTweetsRetrieved, isStopRequested))

      // check if the stream context is still running
      if (! isStreamContextStopped)
      {
        // check if the requested number of tweets has been reached
        if (nbTweetsRetrieved >= nbTweets)
        {
          // check if the stream context has been requested to stop
          if (! isStopRequested)
          {
            println("stopping StreamingContext")

            // stop the stream context
            streamingContext.stop()

            println("StreamingContext stop requested")

            // remember that the stream context has been requested to stop
            isStopRequested = true
          }
        }
      }
    }

    // stop the SparkContext
    sparkContext.stop()
  }

  /** The application main, i.e. its entry point
    *
    * @param args the application parameters
    */
  def main(args : Array[String])
  {
    if (args.length < 4) {
      System.err.println("Usage: Streamer language nbTweets seconds keywords..")
      System.exit(1)
    }

    // extract tweet language
    val language = args(0)

    // extract the number of tweets to retrieved (minimum)
    val nbTweets = args(1).toInt

    // extract the stream context batch duration
    val seconds = args(2).toInt

    // starting from the 3rd, all parameters are tweets keywords
    val keywords=args.drop(3)

    // Store Twitter credentials as System Properties
    System.setProperty("twitter4j.oauth.consumerKey", "")
    System.setProperty("twitter4j.oauth.consumerSecret", "")
    System.setProperty("twitter4j.oauth.accessToken", "")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "")

    // serve the tweet retrieval request
    retrieveTweets(nbTweets, Seconds(seconds), language, keywords)
  }
}
