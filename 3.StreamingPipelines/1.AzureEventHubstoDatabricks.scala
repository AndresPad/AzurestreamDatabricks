// Databricks notebook source
// MAGIC %md # <img src ='https://airsblobstorage.blob.core.windows.net/airstream/Asset 256.png' width="50px"> Stream data into Azure Databricks using Event Hubs
// MAGIC 
// MAGIC This notebook streams tweets with the keyword "Azure" into Event Hubs in real time.
// MAGIC 
// MAGIC (Twitter API has certain request restrictions and quotas. If you are not satisfied with standard rate limiting in Twitter API, you can generate text content without using Twitter API in this example. To do that, set variable dataSource to test instead of twitter and populate the list testSource with preferred test input.)
// MAGIC 
// MAGIC [link to tutorial](https://docs.microsoft.com/en-us/azure/databricks/scenarios/databricks-stream-from-eventhubs)

// COMMAND ----------

import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

val namespaceName = "YOUREVENTHUBNMESPACE"
val eventHubName = "YOUREVENTHUB"
val sasKeyName = "YOURSASSIGNITURENAME"
val sasKey = "YOURSASKEY"

val connStr = new ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val pool = Executors.newScheduledThreadPool(1)
val eventHubClient = EventHubClient.createFromConnectionString(connStr.toString(), pool)

def sleep(time: Long): Unit = Thread.sleep(time)

def sendEvent(message: String, delay: Long) = {
  sleep(delay)
  val messageData = EventData.create(message.getBytes("UTF-8"))
  eventHubClient.get().send(messageData)
  System.out.println("Sent event: " + message + "\n")
}

// Add your own values to the list
//val testSource = List("My computer is Azure", "Azure is the greatest!", "Azure isn't working :(", "Azure is okay.", "Andres Loves Azure!", "The world runs on Azure!")
val testSource = List("Andres is the greatest!", "Azure isn't working :(", "Azure is okay.")

// Specify 'test' if you prefer to not use Twitter API and loop through a list of values you define in `testSource`
// Otherwise specify 'twitter'
val dataSource = "twitter"

if (dataSource == "twitter") {

  import twitter4j._
  import twitter4j.TwitterFactory
  import twitter4j.Twitter
  import twitter4j.conf.ConfigurationBuilder

  // Twitter configuration!
  // Replace values below with you
  val twitterConsumerKey = "YOURCONSUMERKEY"
  val twitterConsumerSecret = "YOURCONSUMERSECRET"
  val twitterOauthAccessToken = "YOURACCESSTOKEN"
  val twitterOauthTokenSecret = "YOURSTOKENSECRET"
  
  val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
    .setOAuthConsumerKey(twitterConsumerKey)
    .setOAuthConsumerSecret(twitterConsumerSecret)
    .setOAuthAccessToken(twitterOauthAccessToken)
    .setOAuthAccessTokenSecret(twitterOauthTokenSecret)

  val twitterFactory = new TwitterFactory(cb.build())
  val twitter = twitterFactory.getInstance()

  // Getting tweets with keyword "Azure" and sending them to the Event Hub in realtime!
  val query = new Query(" #Azure ")
  query.setCount(100)
  query.lang("en")
  var finished = false
  while (!finished) {
    val result = twitter.search(query)
    val statuses = result.getTweets()
    var lowestStatusId = Long.MaxValue
    for (status <- statuses.asScala) {
      if(!status.isRetweet()){
        sendEvent(status.getText(), 100)
      }
      lowestStatusId = Math.min(status.getId(), lowestStatusId)
    }
    query.setMaxId(lowestStatusId - 1)
  }

} else if (dataSource == "test") {
  // Loop through the list of test input data
  while (true) {
    testSource.foreach {
      sendEvent(_,100)
    }
  }

} else {
  System.out.println("Unsupported Data Source. Set 'dataSource' to \"twitter\" or \"test\"")
}

// Closing connection to the Event Hub
eventHubClient.get().close()

// COMMAND ----------

// MAGIC %md # Read tweets from Event Hubs
// MAGIC In the ReadTweetsFromEventHub notebook, paste the following code, and replace the placeholder with values for your Azure Event Hubs that you created earlier. This notebook reads the tweets that you earlier streamed into Event Hubs using the SendTweetsToEventHub notebook.

// COMMAND ----------

import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._

// Build connection string with the above information
val namespaceName = "YOUREVENTHUBNMESPACE"
val eventHubName = "YOUREVENTHUB"
val sasKeyName = "YOURSASSIGNITURENAME"
val sasKey = "YOURSASKEY"

val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val customEventhubParameters =
  EventHubsConf(connStr.toString())
  .setConsumerGroup("databricks-cg")
  .setMaxEventsPerTrigger(5)

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

incomingStream.printSchema

// Sending the incoming stream into the console.
// Data comes in batches!
incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// MAGIC %md Because the output is in a binary mode, use the following snippet to convert it into string.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Event Hub message format is JSON and contains "body" field
// Body is binary, so we cast it to string to see the actual content of the message
val messages =
  incomingStream
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
  .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
  .withColumn("Body", $"body".cast(StringType))
  .select("Offset", "Time (readable)", "Timestamp", "Body")

messages.printSchema

messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// MAGIC %md That's it! Using Azure Databricks, you have successfully streamed data into Azure Event Hubs in near real-time. You then consumed the stream data using the Event Hubs connector for Apache Spark. 
