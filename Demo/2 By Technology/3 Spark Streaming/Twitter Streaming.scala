// Databricks notebook source exported at Wed, 14 Sep 2016 16:56:41 UTC
// MAGIC %md
// MAGIC 
// MAGIC # Twitter Hashtag Count
// MAGIC 
// MAGIC Using Twitter Streaming is a great way to learn Spark Streaming if you don't have your streaming datasource and want a great rich input dataset to try Spark Streaming transformations on.
// MAGIC 
// MAGIC In this example, we show how to calculate the top hashtags seen in the last X window of time every Y time unit.

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.math.Ordering

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Step 1: Enter your Twitter API Credentials.
// MAGIC * Go to https://apps.twitter.com and look up your Twitter API Credentials, or create an app to create them.
// MAGIC * Run this cell for the input cells to appear.
// MAGIC * Enter your credentials.
// MAGIC * Run the cell again to pick up your defaults.

// COMMAND ----------

dbutils.widgets.text("twitter_consumer_key", "", "1. Consumer Key (API Key)")
dbutils.widgets.text("twitter_consumer_secret", "", "2. Consumer Secret (API Secret)")
dbutils.widgets.text("twitter_access_token", "", "3. Access Token")
dbutils.widgets.text("twitter_access_secret", "", "4. Access Token Secret")
dbutils.widgets.text("slide_interval", "5", "5. Slide Interval - Recompute the top hashtags every N seconds")
dbutils.widgets.text("window_length", "600", "6. Window Length - Compute the top hashtags for the last N seconds")

// COMMAND ----------

System.setProperty("twitter4j.oauth.consumerKey", dbutils.widgets.get("twitter_consumer_key"))
System.setProperty("twitter4j.oauth.consumerSecret", dbutils.widgets.get("twitter_consumer_secret"))
System.setProperty("twitter4j.oauth.accessToken", dbutils.widgets.get("twitter_access_token"))
System.setProperty("twitter4j.oauth.accessTokenSecret", dbutils.widgets.get("twitter_access_secret"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Step 2: Configure where to output the top hashtags and how often to compute them.
// MAGIC * Run this cell for the input cells to appear.
// MAGIC * Enter your credentials.
// MAGIC * Run the cell again to pick up your defaults.

// COMMAND ----------

val slideInterval = new Duration(dbutils.widgets.get("slide_interval").toInt * 1000)
val windowLength = new Duration(dbutils.widgets.get("window_length").toInt * 1000)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Step 3: Run the Twitter Streaming job.

// COMMAND ----------

// MAGIC %md Create the function to that creates the Streaming Context and sets up the streaming job.

// COMMAND ----------

var newContextCreated = false
var num = 0

// This is a helper class used for 
object SecondValueOrdering extends Ordering[(String, Int)] {
  def compare(a: (String, Int), b: (String, Int)) = {
    a._2 compare b._2
  }
}

//case class WordCount(word: String, count: Int)

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  
  // Create a Spark Streaming Context.
  val ssc = new StreamingContext(sc, slideInterval)
  
  // Create a Twitter Stream for the input source. 
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth)
  
  // Parse the tweets and gather the hashTags.
  //val hashTagStream = twitterStream.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
  
  val englishWordStream = twitterStream.filter(_.getLang.equals("en")).map(_.getText).flatMap(_.split(" "))
  
  // Compute the counts of the words by window.
  val windowedCountStream = englishWordStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, slideInterval)

  // For each window, calculate the top hashtags for that time period.
  windowedCountStream.foreachRDD(countRDD => {    
    countRDD.toDF("word", "count").registerTempTable("twitter_word_count")    
    
    countRDD.filter(_._1.startsWith("#")).toDF("word", "count").registerTempTable("twitter_hashtag_count")    
  })
  
  // To make sure data is not deleted by the time we query it interactively
  ssc.remember(Minutes(10)) 
  
  println("Creating function called to create new StreamingContext")
  newContextCreated = true
  ssc
}

// COMMAND ----------

// MAGIC %md Create the StreamingContext using getActiveOrCreate, as required when starting a streaming job in Databricks.

// COMMAND ----------

@transient val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Start the Spark Streaming Context and return when the Streaming job exits or return with the specified timeout.  

// COMMAND ----------

ssc.start()
ssc.awaitTerminationOrTimeout(5 * 1000)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Step 4: View the Results.

// COMMAND ----------

// MAGIC %sql select * from twitter_word_count order by count desc

// COMMAND ----------

// MAGIC %sql select * from twitter_hashtag_count order by count desc

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Stop any active Streaming Contexts, but don't stop the spark contexts they are attached to.

// COMMAND ----------

StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Example Tweet:
// MAGIC 
// MAGIC {createdAt=Wed Sep 07 01:32:40 UTC 2016,
// MAGIC id=773333129901830144,
// MAGIC text='RT @latidosdelrock: Siempre se volverá al primer amor.',
// MAGIC source='<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>',
// MAGIC isTruncated=false,
// MAGIC inReplyToStatusId=-1,
// MAGIC inReplyToUserId=-1,
// MAGIC isFavorited=false, 
// MAGIC isRetweeted=false, 
// MAGIC favoriteCount=0, 
// MAGIC inReplyToScreenName='null', 
// MAGIC geoLocation=null, 
// MAGIC place=null, 
// MAGIC retweetCount=0, 
// MAGIC isPossiblySensitive=false, 
// MAGIC lang='es', 
// MAGIC contributorsIDs=[],
// MAGIC retweetedStatus=StatusJSONImpl{createdAt=Wed Sep 07 01:30:07 UTC 2016, id=773332489376903168, text='Siempre se volverá al primer amor.', source='<a href="https://about.twitter.com/products/tweetdeck" rel="nofollow">TweetDeck</a>', isTruncated=false, inReplyToStatusId=-1, inReplyToUserId=-1, isFavorited=false, isRetweeted=false, favoriteCount=170, inReplyToScreenName='null', geoLocation=null, place=null, retweetCount=146, isPossiblySensitive=false, lang='es', contributorsIDs=[], retweetedStatus=null, userMentionEntities=[], urlEntities=[], hashtagEntities=[], mediaEntities=[], symbolEntities=[], currentUserRetweetId=-1, user=UserJSONImpl{id=1222401072, name='Latidos del Rock', screenName='latidosdelrock', location='Buenos Aires', description='Las frases que marcaron cada momento de tu vida | Facebook: https://t.co/H9ysKYcuxT | Instagram: https://t.co/N2ilFb0VeP | Contacto: latidosdelrock@yahoo.com.ar', isContributorsEnabled=false, profileImageUrl='http://pbs.twimg.com/profile_images/449423079467257856/VWlWfAWg_normal.jpeg', profileImageUrlHttps='https://pbs.twimg.com/profile_images/449423079467257856/VWlWfAWg_normal.jpeg', isDefaultProfileImage=false, url='https://www.facebook.com/latidosdelrockok', isProtected=false, followersCount=585124, status=null, profileBackgroundColor='131516', profileTextColor='333333', profileLinkColor='009999', profileSidebarFillColor='DDEEF6', profileSidebarBorderColor='FFFFFF', profileUseBackgroundImage=false, isDefaultProfile=false, showAllInlineMedia=false, friendsCount=39301, createdAt=Tue Feb 26 16:53:37 UTC 2013, favouritesCount=5819, utcOffset=-10800, timeZone='Buenos Aires', profileBackgroundImageUrl='http://pbs.twimg.com/profile_background_images/449427005180612608/3Ebntm-E.jpeg', profileBackgroundImageUrlHttps='https://pbs.twimg.com/profile_background_images/449427005180612608/3Ebntm-E.jpeg', profileBackgroundTiled=false, lang='es', statusesCount=24546, isGeoEnabled=true, isVerified=false, translator=false, listedCount=252, isFollowRequestSent=false, withheldInCountries=null}, withHeldInCountries=null, quotedStatusId=-1, quotedStatus=null},
// MAGIC 
// MAGIC userMentionEntities=[UserMentionEntityJSONImpl{name='Latidos del Rock', screenName='latidosdelrock', id=1222401072}],
// MAGIC urlEntities=[],
// MAGIC hashtagEntities=[],
// MAGIC mediaEntities=[],
// MAGIC symbolEntities=[],
// MAGIC currentUserRetweetId=-1,
// MAGIC 
// MAGIC user=UserJSONImpl{id=764290190584217600, name='002.', screenName='carlaibanez01', location='Neuquén, Argentina', description='tu sonrisa & perdí por goleada ❣ || ✖ Después del rock, tu amor y él vino✖ || snap - carlaibanez7_7 || contra él mundo por vos ||', isContributorsEnabled=false, profileImageUrl='http://pbs.twimg.com/profile_images/772860749177815041/XK1zQnie_normal.jpg', profileImageUrlHttps='https://pbs.twimg.com/profile_images/772860749177815041/XK1zQnie_normal.jpg', isDefaultProfileImage=false, url='https://www.instagram.com/carlaibanez01/', isProtected=false, followersCount=391, status=null, profileBackgroundColor='000000', profileTextColor='000000', profileLinkColor='981CEB', profileSidebarFillColor='000000', profileSidebarBorderColor='000000', profileUseBackgroundImage=false, isDefaultProfile=false, showAllInlineMedia=false, friendsCount=531, createdAt=Sat Aug 13 02:39:16 UTC 2016, favouritesCount=1913, utcOffset=-25200, timeZone='Pacific Time (US & Canada)', profileBackgroundImageUrl='http://abs.twimg.com/images/themes/theme1/bg.png', profileBackgroundImageUrlHttps='https://abs.twimg.com/images/themes/theme1/bg.png', profileBackgroundTiled=false, lang='es', statusesCount=2542, isGeoEnabled=true, isVerified=false, translator=false, listedCount=0, isFollowRequestSent=false, withheldInCountries=null}, 
// MAGIC 
// MAGIC withHeldInCountries=null, 
// MAGIC quotedStatusId=-1, 
// MAGIC quotedStatus=null}

// COMMAND ----------

