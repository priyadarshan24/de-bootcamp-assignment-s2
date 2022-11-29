import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.{io, util}
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

object SimpleApp {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://Priyadarshans-MacBook-Pro-2.local:7077").setAppName("Simple Application")
    val spark = SparkSession.builder.config(conf).master("spark://Priyadarshans-MacBook-Pro-2.local:7077").appName("Simple Application").config("spark.sql.warehouse.dir", "/Users/priyadarshanp/myOpensourceContribution/DEBootCamp_GitVersion/de-bootcamp-assignment-s2/spark-warehouse").getOrCreate()

      val toBeProcessedFolderLocation = "/Users/priyadarshanp/Documents/DEBootCamp/files/toBeProcessed"
    val processedFolderLocation = "/Users/priyadarshanp/Documents/DEBootCamp/files/Processed"
    val reportFolderLocation = "/Users/priyadarshanp/Documents/DEBootCamp/files/Reports"
    val sortedFolderMap = sortFolderNames("/Users/priyadarshanp/Documents/DEBootCamp/files/toBeProcessed");

    try {
      sortedFolderMap.forEach {
        case (key, folderName) => {

          val processedTargetFolder = Files.createDirectories(Paths.get(String.format("%s/%s", processedFolderLocation, folderName)));
          val toBeProcessedSourceFolder = Files.createDirectories(Paths.get(String.format("%s/%s", toBeProcessedFolderLocation, folderName)))
          //val sourcedToTable = "ga_events"

          val sourcedToTable = loadFileToBeProcessedInDataRepo(toBeProcessedSourceFolder, spark)
          moveProcessedFilesToProcessedFolder(toBeProcessedSourceFolder, processedTargetFolder)

          val reportFolders = Files.createDirectories(Paths.get(String.format("%s/%s/%s", reportFolderLocation, LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE), folderName)));

          //Report 1 - Run Spark job against all the all Data Repo Folder to generate reports
          runSparkJobToGenerateTopContentViewReport(spark, sourcedToTable, reportFolders.toAbsolutePath.toString, folderName)

          val unProcessedDirectoryToDelete = new Directory(new File(toBeProcessedSourceFolder.toAbsolutePath.toString))

          unProcessedDirectoryToDelete.deleteRecursively()

       /*   //Report 2 - Run Spark job against all the all Data Repo Folder to generate reports
          runSparkJobToGeneratePreferredPlatformReport(spark, inputDataFrame, reportFolders.toAbsolutePath.toString)*/

          //val unProcessedDirectoryToDelete = new Directory(new File(toBeProcessedSourceFolder.toAbsolutePath.toString))
          //unProcessedDirectoryToDelete.deleteRecursively()
        }
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        //runCleanUp(filesToBeCleanedUp.toList)
      }
    }
  }

  private def sortFolderNames(dirPath: String): util.TreeMap[LocalDate, String] = {

    val folderDateTimeMap = new util.TreeMap[LocalDate, String]()
    new io.File(dirPath).listFiles.filter(_.isDirectory).map(_.getName)
      .map(folderName => folderDateTimeMap.put(LocalDate.parse(folderName, DateTimeFormatter.ofPattern("yyyy-MM-dd")), folderName)).toList;
    folderDateTimeMap;
  }

  private def loadFileToBeProcessedInDataRepo(folderToBeProcessed : Path, spark: SparkSession) : String = {
    folderToBeProcessed.toFile.listFiles(_.isFile).filter(!_.getName.contains(".DS_Store")).foreach(curFile=> {
      println("AbsolutePath:::" + curFile.getAbsolutePath)
      //val processedFileDataFrame = spark.read.json("/Users/priyadarshanp/Documents/DEBootCamp/files/Processed/2022-01-14/2eabd773-03d8-fd78-49e5-4440c0771911.json.gz")
      //val processedFileDataFrame = spark.read.json("/Users/priyadarshanp/Documents/DEBootCamp/files/Processed/2022-01-14/5dd60ffd-4681-33a6-b847-fb32745599bc.json.gz")
      val processedFileDataFrame = spark.read.json(curFile.getAbsolutePath)
      processedFileDataFrame.createOrReplaceTempView("googleAnalyticsView")
      val processedFileDataFrameWithDateTimeStamp = spark.sql("select *, DATE(timestamp) as eventDate from googleAnalyticsView")
      processedFileDataFrameWithDateTimeStamp
        .write
        .option("path", "/Users/priyadarshanp/myOpensourceContribution/DEBootCamp_GitVersion/de-bootcamp-assignment-s2/spark-warehouse")
        .partitionBy("eventDate")
        .mode(SaveMode.Append)
        .format("parquet").saveAsTable("ga_events")
    })
    "ga_events"
  }

  private def moveProcessedFilesToProcessedFolder(sourceFolder:Path, processedFolder: Path) : Unit = {
      sourceFolder.toFile.listFiles().filter(!_.getName.contains(".DS_Store")).foreach(curFile => {
      val sourceFileToBeMoved = Paths.get(String.format("%s/%s", sourceFolder.toAbsolutePath().toString, curFile.getName))
      Files.copy(sourceFileToBeMoved, Paths.get(String.format("%s/%s", processedFolder.toAbsolutePath.toString, curFile.getName)), StandardCopyOption.REPLACE_EXISTING)
    })
  }

    private def runCleanUp(cleanUpFiles: List[Path]): Unit = {
      cleanUpFiles.foreach(path => {
        Files.deleteIfExists(path)

        if (path.toAbsolutePath.toString.contains("Processed") && (new File(path.getParent.toAbsolutePath.toString)).listFiles().toList.isEmpty) {
          Files.delete(path.getParent)
        }
      })
    }

    //spark.sql("select count(*),eventDate from analyticsView20220101 group by eventDate").show(200)


    private def runSparkJobToGenerateTopContentViewReport(spark: SparkSession, tableName: String, targetReportFolderPath: String, reportDate: String) = {
      val reportNameTopContent = String.format("%s_%s", "topContent_", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
      val reportNameDir = String.format("%s/%s", targetReportFolderPath, "topContent")
      val reportPath = String.format("%s/%s.%s", reportNameDir, reportNameTopContent, "csv")

      val sparkQueryToExecute = String.format("select SUBSTRING_INDEX(context.page.title, '-', 1) as realtitle, sum(properties.video_length) as totalLengthOfVideoWatched  from %s where event = 'Watched Video' and  eventDate > date_add(DATE('%s'), -7) and eventDate <= DATE('%s') group by realtitle sort by totalLengthOfVideoWatched desc", tableName, reportDate, reportDate);
      println("Printing sparkQueryToExecute:" + sparkQueryToExecute)
      var popularContentByTileBasisVideoLengthWatched = spark.sql(sparkQueryToExecute)
      popularContentByTileBasisVideoLengthWatched.repartition(1).write.mode(SaveMode.Overwrite).option("header", true).csv(reportNameDir)

      val list = Files.createDirectories(Paths.get(reportNameDir)).toFile.listFiles().filter(_.getName.startsWith("part")).toList
      Files.move(Paths.get(list(0).getAbsolutePath), Paths.get(reportPath), StandardCopyOption.ATOMIC_MOVE)
    }

    private def runSparkJobToGeneratePreferredPlatformReport(spark: SparkSession, inputDataFrame: DataFrame, targetReportFolderPath: String) = {
      val reportNameTopContent = String.format("%s_%s", "preferredPlatform_", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
      val reportNameDir = String.format("%s/%s", targetReportFolderPath, "preferredPlatform")
      val reportPath = String.format("%s/%s.%s", reportNameDir, reportNameTopContent, "csv")

      inputDataFrame.printSchema()
      inputDataFrame.createOrReplaceTempView("analyticsView");
      val sparkQueryToExecute = "select context.traits.platform, count(distinct userid) as noOfUsers from analyticsView where event = 'Watched Video' group by context.traits.platform sort by noOfUsers desc"
      var preferredPlatform = spark.sql(sparkQueryToExecute)
      preferredPlatform.repartition(1).write.mode(SaveMode.Overwrite).option("header", true).csv(reportNameDir)

      val list = Files.createDirectories(Paths.get(reportNameDir)).toFile.listFiles().filter(_.getName.startsWith("part")).toList
      Files.move(Paths.get(list(0).getAbsolutePath), Paths.get(reportPath), StandardCopyOption.ATOMIC_MOVE)
    }
}
/*
val inputDataFrame = spark.read.json("/Users/priyadarshanp/Documents/DEBootCamp/files/toBeProcessed/2022-01-14/9936d58b-190c-f03e-0ce1-21b4dd7f4da0.json.gz")
val inputDataFrame1401 = spark.read.json("/Users/priyadarshanp/Documents/DEBootCamp/files/toBeProcessed/2022-01-14/2eabd773-03d8-fd78-49e5-4440c0771911.json.gz")
inputDataFrame1401.createOrReplaceTempView("analyticsView1401")
val inputDataFramewithTimestamp1401 = spark.sql("select *, DATE(timestamp) as eventDate from analyticsView1401")
inputDataFramewithTimestamp1401.createOrReplaceTempView("analyticsViewWithTimeStamp1401")
spark.sql("select count(*),eventDate from analyticsViewWithTimeStamp1401 group by eventDate sort by eventDate").show(200)
inputDataFramewithTimestamp1401.write.partitionBy("eventDate").format("json").save("dataByTimestamp.json")

val inputDataFramewithTimestamp1401 = spark.sql("select *, DATE(timestamp) as eventDate from analyticsView1401")

 */
/*
val inputDataFrame = spark.read.json("/Users/priyadarshanp/Documents/DEBootCamp/files/Processed/allFiles/5dd60ffd-4681-33a6-b847-fb32745599bc.json.gz")
inputDataFrame.createOrReplaceTempView("analyticsView")
spark.sql("select distinct DATE(timestamp) from analyticsView sort by DATE(timestamp) asc").show(200)
 */