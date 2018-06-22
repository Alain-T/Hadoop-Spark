// Start a new Hive Context called hc
val hc = new org.apache.spark.sql.hive.HiveContext(sc)

// use hc.sql().show to show databases available. (You need to do .show to execute it because of the lazy evaluation of spark)
hc.sql("show databases").show

// use hc.sql().show to use your database
hc.sql("use tweet_db").show
hc.sql("show tables").show

// use hc.sql().limit(n).show to show n first records of your table
hc.sql("select * from tweets").limit(2).show

// Enter the following command. It allow you to cast Dataframe into a RDD of object.
import hc.implicits._

// Create a new case class named tweet. tweet attributes must be exactly the same name as your column name of your table in hive.
case class tweet(
tweedId                  : String,
text                     : String,
retweetCount             : Int,
mediaEntitiesCount       : Int,
urlEntitiesCount         : Int,
userMentionEntitiesCount : Int,
userId                   : String,
userScreenName           : String,
userFollowersCount       : Int,
userFriendsCount         : Int,
time                     : BigInt
)

// Use hc.sql().as[tweet].rdd create a rdd of tweet.
val tweets_df = hc.sql("select * from tweets")
val tweets_rdd=tweets_df.as[tweet].rdd

// Process the data

// import required to use SparkML
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

// extract the numerical part of the tweets
val rdd_vect = tweets_rdd.map(tweet => {
Vectors.dense(
tweet.text.length,
tweet.retweetCount,
tweet.mediaEntitiesCount,
tweet.urlEntitiesCount,
tweet.userMentionEntitiesCount,
tweet.userScreenName.length,
tweet.userFollowersCount,
tweet.userFriendsCount)
})

// copute basic statistics on the column
val summary: MultivariateStatisticalSummary = Statistics.colStats(rdd_vect)

println(summary.mean) // a dense vector containing the mean value for each column
println(summary.variance) // column-wise variance
println(summary.numNonzeros) // number of nonzeros in each column