import org.apache.spark.sql.functions.col
import scalaj.http._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.math.Ordering

object Spotify {
  def main(args: Array[String]): Unit = {

    val accessToken = "BQDMFzmMU00FNV1tLOnleSZhq028ExrocKB70gvlvoB9PzeIc4H7r_KgcwLqn30EyhF1vnfV7uh47dvVYYphm8xpN5r8NN9FjkzNlq-sn0nyJZVwaxWnOTnVoxvHxUSt67UD2fW-40KHvPgbUXTgH8jX79Zc4v8D4cEhuA3L-eVhh2ij7RZL3ZmE7xNQOBYaRlrY";
    val playlistId = "5Rrf7mqN8uus2AaQQQNdc1"
    val searchUrl = s"https://api.spotify.com/v1/playlists/${playlistId}/tracks"

    val limit = 50
    implicit val formats: Formats = DefaultFormats

    var duration: List[Any] = Nil
    var name: List[Any] = Nil
    var id: List[Any] = Nil

    for(i<-0 to 9){
      val offset = i*50
      val response = Http(searchUrl)
        .header("Authorization", s"Bearer ${accessToken}")
        .param("offset", offset.toString)
        .param("limit", limit.toString)
        .asString
      val json = parse(response.body)
      val c = (json \ "items" \ "track" \ "name").extract[List[String]]
      val d = (json \ "items" \ "track" \ "duration_ms").extract[List[Int]]
      val x = (json \ "items" \ "track" \ "artists" \ "id").extract[List[String]]
      id = id:::x
      name = name:::c
      duration = duration:::d
    }

    println(id)
    println(name)
    println(duration)
    val data = id.zip(duration).zip(name).map { case ((id, duration), name) => (id, duration, name) }
    val rows = data.map { case (id, duration, name) => Row(id, duration, name) }

    val spark = SparkSession.builder()
      .appName("Example")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("duration", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val sortedDF = df.orderBy(col("duration").desc)

    sortedDF.limit(10).show()
    val idList = sortedDF.limit(10).select(col("id")).collect.map(row => row(0).asInstanceOf[String]).toList
    println(idList)
    var artist: List[String] = Nil
    var follower: List[Int] = Nil
    val accessTokenforArtist = "BQCkWlOKhWV7dmOwESJVl3QXzGppCLQcMvmbs60QZITRlnDNDqEE0uGnpo2Jy9UNoCvNCzj14BTRsnT66HyBvAD1gSVU2uG7lFQ9nMBQYPvVXkXK9GeIjnFLtfsztv63jxYVYI5bOZbA6DDHcGtTBHAnc8ayjs6bs6LSccRGnxxOKEWPYTEXC1H5Wm9RB_IxrC0w";
    for(element<-idList){


      val searchUrl = s"https://api.spotify.com/v1/artists/${element}"
      val response1 = Http(searchUrl)
        .header("Authorization", s"Bearer ${accessTokenforArtist}")
        .asString
      val json1 = parse(response1.body)
      //println(response1)
      val n = (json1 \ "name").extract[String]
      val q = (json1 \ "followers" \ "total").extract[Int]

      artist = artist :+ n
      follower = follower :+ q
    }
    val artist_follower = artist.zip(follower).sortBy(_._2)
    for(element <- artist_follower){
      println(element)
    }
  }
}
