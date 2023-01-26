import courier.Defaults._
import courier._
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.{io, util}
import scala.reflect.io.Directory
import scala.util._

object StreamingApp {


  def main(args: Array[String]) {

    System.out.println("Starting the streamingApp work::")

    val wareHouseLocation = "/Users/priyadarshanp/myOpensourceContribution/DEBootCamp_GitVersion/de-bootcamp-assignment-s2/spark-warehouse"
    val spark = SparkSession.builder.master("spark://Priyadarshans-MacBook-Pro-2.local:7077").appName("StreamingApp").config("spark.sql.warehouse.dir", wareHouseLocation).enableHiveSupport().getOrCreate()

    // Subscribe to 1 topic
  /*  val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "acmeFlixStream")
      .load()
      .select(col("value").cast("json"))*/

    import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
    val acmeFlixStreamingSchema = new StructType().add("deviceId", StringType).add("userId", StringType).add("videoId", IntegerType).add("segmentId", IntegerType)

    var jsonDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "acmeFlixStream")
      .load()
      .select(from_json(col("value").cast("string"), acmeFlixStreamingSchema).alias("parsed_value"))
     // .select(col("parsed_value.videoId"), col("parsed_value.segmentId"))

    jsonDf.createOrReplaceTempView("acmeFlixView")
    val acmeFLixTableView = spark.sql("select parsed_value.deviceId from acmeFlixView")
    acmeFLixTableView
      .writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]


    df
      .writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()

    System.out.println("Read Kafka stream")
    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      //.as[(String, String)]
  }
}