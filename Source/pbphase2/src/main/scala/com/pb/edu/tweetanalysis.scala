package com.pb.edu


import java.lang.System.setProperty
import java.lang.System._

import scala.collection.JavaConversions._
import scala.collection.convert.wrapAll._
import scala.collection.convert.decorateAll._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created on 10/25/17  .
  */
object tweetanalysis {

  def main(args: Array[String]) {



    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // loading the tweetfile

    val jsonFile = sqlContext.jsonFile("data2.json")

    jsonFile.registerTempTable("MainTable")

    //jsonFile.show()

    val lang_refiner = sqlContext.sql("SELECT * FROM MainTable where lang='en'")

    lang_refiner.registerTempTable("Lang_Refiner")

    //lang_refiner.show()


    //retreving hashtags text from Lang_Refiner table

    val table2 = sqlContext.sql("SELECT entities.hashtags.text AS ht FROM MainTable WHERE entities.hashtags.text IS NOT NULL")

    table2.registerTempTable("Table2")

    //getting data from Table2 to split file in to words

    val table21 = sqlContext.sql("SELECT ht FROM Table2").map(l => l.getList(0))

    //converting to single array

    val htd = table21.filter(l => l.size() > 0).flatMap(tags => {
      tags.toArray
    })

    //saving the results

    //htd.saveAsTextFile("HashOut2")

    // retreving the saved result file

    val htd1 = sc.textFile("HashOut2")

    val schemaString = "HashTags"

    //Creating the schema

    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    //Converting RDD into row format to create table

    val rowRDD = htd1.map(r => r.split(",")).map(l => Row(l(0)))

    //Creating data frame with RDD and created schema

    val hashTagDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Creating table from data frame created

    hashTagDataFrame.registerTempTable("Table21")

    //hashTagDataFrame.show()



    val NUMTWEETS = sqlContext.sql("SELECT user.screen_name AS u_sn,count(*) AS u_count FROM Lang_Refiner GROUP BY user.screen_name ORDER BY u_count DESC LIMIT 10")


    NUMTWEETS.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/UserCount")

    NUMTWEETS.registerTempTable("NT")

    //getting the users with highest followers

    val FOLLOWERC=sqlContext.sql("SELECT user.followers_count AS ft,user.screen_name AS ust,count(*) AS f_count FROM Lang_Refiner GROUP BY user.followers_count,user.screen_name ORDER BY f_count DESC LIMIT 50")

    FOLLOWERC.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/FollowersCount")

    FOLLOWERC.registerTempTable("FC")

    //getting the users with highest friends

    val FRIENDC=sqlContext.sql("SELECT user.friends_count AS st,user.screen_name AS ust,count(*) AS fr_count FROM Lang_Refiner GROUP BY user.friends_count,user.screen_name ORDER BY fr_count DESC LIMIT 50")

    FRIENDC.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/FriendsCount")

    FRIENDC.registerTempTable("FR")

    //getting the users with highest statuses

    val STATUSC=sqlContext.sql("SELECT user.statuses_count AS st,user.screen_name AS ust,count(*) AS s_count FROM Lang_Refiner GROUP BY user.statuses_count,user.screen_name ORDER BY s_count DESC LIMIT 10")

    STATUSC.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/StatusesCount")

    STATUSC.registerTempTable("SC")

    //getting the users with highest language used

    val lang_count=sqlContext.sql(" SELECT user.lang, COUNT(*) as cnt FROM MainTable GROUP BY user.lang ORDER BY cnt DESC limit 15")

    lang_count.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/Languages")

    lang_count.registerTempTable("AT")

    //getting the users with respect to location

    val location_refiner=sqlContext.sql("SELECT user.screen_name, user.location FROM Lang_Refiner where user.location IS NOT NULL")

    location_refiner.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/Location")

    location_refiner.registerTempTable("LR")


    //joining the two tables to get popular users with highest followers count

    val join= sqlContext.sql("SELECT NT.u_sn AS N, FC.ft FROM NT " +
      "JOIN FC ON (NT.u_sn = FC.ust) GROUP BY " +
      "NT.u_sn,FC.ft ORDER BY FC.ft DESC")
    join.show()
    join.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/MostPopUsers")

    //getting the users with respect to timezones

    val time_refine=sqlContext.sql("SELECT user.screen_name, user.time_zone FROM Lang_Refiner ORDER BY user.time_zone DESC")

    time_refine.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/Timezone")

    time_refine.registerTempTable("TR")

    val lines= sc.textFile("tweets.txt")

    //getting the users with respect to devices
    val iphone=lines.filter(line=>line.contains("Twitter for iPhone"))
    val android=lines.filter(line=>line.contains("Twitter for Android"))
    val ipad=lines.filter(line=>line.contains("Twitter for iPad"))
    val web=lines.filter(line=>line.contains("Twitter Web Client"))

    //getting the tweets with the different hurricanes

    val hur=lines.filter(line=>line.contains("Hurricane"))
    val irma=lines.filter(line=>line.contains("Irma"))
    val harvey=lines.filter(line=>line.contains("Harvey"))
    val maria=lines.filter(line=>line.contains("Maria"))
    val jose=lines.filter(line=>line.contains("Jose"))
    val pr=lines.filter(line=>line.contains("PuertoRico"))

    println("Iphone users are: " + iphone.count()+ "\nAndroid users are: " + android.count()+ "\nIpad users are: " + ipad.count()+ "\nWeb users are: " + web.count())

    println("Number of tweets on Hurricane: " +hur.count()+"\nNumber of tweets on Irma: " +irma.count()+ "\nNumber of tweets on Harvey: " +harvey.count()+ "\nNumber of tweets on Maria: " +maria.count()+"\nNumber of tweets on Jose: " +jose.count()+"\nNumber of tweets on PuertoRico: " +pr.count())
/*

*/

  }

}

