package io.beanbags.scalaproject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.log4j._
import java.time.LocalTime
import java.time.Duration

object moviesRatings {
  // This line hides [Info] and [Warn] from log.
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit= {
    /*====================================================================================================*/
    /*====================================== Configure SparkContext ======================================*/
    /*====================================================================================================*/
    val sc = new SparkContext(new SparkConf().setAppName("Movies Rater").setMaster("local[*]"))
    //val sc = new SparkContext(new SparkConf())

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
      .load("datasets/ratings.csv")
      .drop("userId", "timestamp")

    println("Reading movies.csv at: " + LocalTime.now())
    val movies = sqlContext.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("inferSchema", "true")
      .load("datasets/movies.csv")

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
      .save("Movies Ratings.csv")

    var endTime = LocalTime.now()
    println("Finished all operations at: " + endTime)
    println("Total time is: " + Duration.between(startTime, endTime).toMinutes + " minutes")
  }
}