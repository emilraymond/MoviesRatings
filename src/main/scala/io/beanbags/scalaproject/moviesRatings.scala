package io.beanbags.scalaproject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.log4j._
import java.time.{Duration, LocalTime}

import org.apache.commons.lang.time
import org.apache.commons.lang.time.DurationFormatUtils

object moviesRatings {
  // This line hides [Info] and [Warn] from log.
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit= {
    println("          ============================================================")
    println("          ================== Application Is Running ==================")
    println("          ============================================================")

    /*====================================================================================================*/
    /*====================================== Configure SparkContext ======================================*/
    /*====================================================================================================*/
    //val sc = new SparkContext(new SparkConf().setAppName("Movies Rater").setMaster("local[*]"))
    val sc = new SparkContext(new SparkConf())

    val sqlContext = new SQLContext(sc)

    /*====================================================================================================*/
    /*============================================ Input File ============================================*/
    /*====================================================================================================*/
    var startTime = LocalTime.now()
    println("Reading ratings.csv at: " + startTime)
    val ratings = sqlContext.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("inferSchema", "true")
      .load("hdfs://localhost:9000/datasets/ratings.csv")
      .drop("userId", "timestamp")

    println("Reading movies.csv at: " + LocalTime.now())
    val movies = sqlContext.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("inferSchema", "true")
      .load("hdfs://localhost:9000/datasets/movies.csv")

    /*====================================================================================================*/
    /*========================================== Main Operation ==========================================*/
    /*====================================================================================================*/
    println("Working on result at: " + LocalTime.now())
    //val result = ratings.join(movies,Seq("movieId")).groupBy("title").avg("rating").orderBy(desc("avg(rating)"))
    val result = ratings
      .join(movies,Seq("movieId"))
      .groupBy("title")
      .agg(avg("rating")
        .alias("Average rating"))
      .orderBy(desc("Average rating"))

    /*====================================================================================================*/
    /*============================================ Output File ===========================================*/
    /*====================================================================================================*/
    println("Writing result at: " + LocalTime.now())
    result
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("hdfs://localhost:9000/output/result.csv")

    var endTime = LocalTime.now()
    println("Finished all operations at: " + endTime)

    /*====================================================================================================*/
    /*========================================== Print Duration ==========================================*/
    /*====================================================================================================*/
    var duration = Duration.between(startTime, endTime).getSeconds()
    var sec = duration % 60
    var min = (duration / 60) % 60
    var hrs = duration / 3600
    println("Total time is: " + hrs + " hours, " + min + " minutes and " + sec + " seconds.")
  }
}