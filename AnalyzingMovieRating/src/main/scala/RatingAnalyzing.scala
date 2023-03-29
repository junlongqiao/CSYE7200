import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object RatingAnalyzing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("MyApp")
      .setMaster("local[*]")
      .set("spark.executor.memory", "16g")

    val spark = SparkSession.builder()
      .config(conf)
      .appName("Example")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read.option("header", "true")
      .option("inferSchema", "true")
      .csv("/Users/junlongqiao/data/Netflix_Dataset_Rating.csv")

    //get the csv file from https://www.kaggle.com/datasets/rishitjavia/netflix-movie-rating-dataset

    import spark.implicits._

    val stdRating = df.select(stddev("Rating")).as[Double].first

    val meanRating = df.select(mean("Rating")).as[Double].first

    println(s"Mean of Rating is: $meanRating")

    println(s"Standard deviation of Rating is: $stdRating")
  }
}
