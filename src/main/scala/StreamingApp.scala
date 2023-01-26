import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

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
      .option("subscribe", "eventStream2")
      .load()
      .select(from_json(col("value").cast("string"), acmeFlixStreamingSchema).alias("parsed_valuev2"))

    jsonDf
      .writeStream
      .outputMode("Append")
      .format("console")
      .start()


    jsonDf.createOrReplaceTempView("acmeFlixView")
    val acmeFLixTableView = spark.sql("select count(*), parsed_valuev2.videoId, parsed_valuev2.segmentId from acmeFlixView group by parsed_valuev2.videoId, parsed_valuev2.segmentId")
    acmeFLixTableView
      .writeStream
      .outputMode("Update")
      .format("console")
      .start()


    /*jsonDf
      .writeStream
      .outputMode("Append")
      .format("json")
      .option("checkpointLocation", "/Users/priyadarshanp/myOpensourceContribution/airflow/de-bootcamp-assignment-s2/checkpoint")
      .option("path", "/Users/priyadarshanp/myOpensourceContribution/airflow/de-bootcamp-assignment-s2")
      .start()*/
    System.out.println("Read Kafka stream")
  }
}