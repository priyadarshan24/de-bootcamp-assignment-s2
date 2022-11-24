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
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val toBeProcessedFolderLocation = "/Users/priyadarshanp/Documents/DEBootCamp/files/toBeProcessed"
    val processedFolderLocation = "/Users/priyadarshanp/Documents/DEBootCamp/files/Processed"
    val reportFolderLocation = "/Users/priyadarshanp/Documents/DEBootCamp/files/Reports"
    val sortedFolderMap = sortFolderNames("/Users/priyadarshanp/Documents/DEBootCamp/files/toBeProcessed");
    val allDataRepoFolder = "/Users/priyadarshanp/Documents/DEBootCamp/files/allFileRepo"
    val allDataRepoFolderPath = Paths.get(allDataRepoFolder)
    val filesToBeCleanedUp = ListBuffer[Path]()
    try {
      sortedFolderMap.forEach {
        case (key, value) => {

          val folderName = value;


          val processedTargetFolder = Files.createDirectories(Paths.get(String.format("%s/%s", processedFolderLocation, folderName)));
          val toBeProcessedSourceFolder = Files.createDirectories(Paths.get(String.format("%s/%s", toBeProcessedFolderLocation, folderName)))

          toBeProcessedSourceFolder.toFile.listFiles().foreach(curFile => {
            val sourceFileToBeMoved = Paths.get(String.format("%s/%s", toBeProcessedSourceFolder.toAbsolutePath().toString, curFile.getName))
            filesToBeCleanedUp += Files.copy(sourceFileToBeMoved, Paths.get(String.format("%s/%s", allDataRepoFolderPath.toAbsolutePath.toString, curFile.getName)), StandardCopyOption.REPLACE_EXISTING)
            filesToBeCleanedUp += Files.copy(sourceFileToBeMoved, Paths.get(String.format("%s/%s", processedTargetFolder.toAbsolutePath.toString, curFile.getName)), StandardCopyOption.REPLACE_EXISTING)
          })

          val dataRepoFilePath = String.format("%s/%s", allDataRepoFolder, "*.gz")
          val inputDataFrame = spark.read.json(String.format("%s/%s", allDataRepoFolder, "*.gz"))

          val reportFolders = Files.createDirectories(Paths.get(String.format("%s/%s", reportFolderLocation, folderName)));

          //Report 1 - Run Spark job against all the all Data Repo Folder to generate reports
          runSparkJobToGenerateTopContentViewReport(spark, inputDataFrame, reportFolders.toAbsolutePath.toString, folderName)

          //Report 2 - Run Spark job against all the all Data Repo Folder to generate reports
          runSparkJobToGeneratePreferredPlatformReport(spark, inputDataFrame, reportFolders.toAbsolutePath.toString)

          val unProcessedDirectoryToDelete = new Directory(new File(toBeProcessedSourceFolder.toAbsolutePath.toString))
          unProcessedDirectoryToDelete.deleteRecursively()
        }
      }
      println("Files to be cleaned::" + filesToBeCleanedUp)

    } catch {
      case e: Exception => {
        e.printStackTrace()
        runCleanUp(filesToBeCleanedUp.toList)
      }
    }

  }

  private def runCleanUp(cleanUpFiles: List[Path]): Unit = {
    cleanUpFiles.foreach(path => {
      Files.deleteIfExists(path)

      if (path.toAbsolutePath.toString.contains("Processed")  && (new File(path.getParent.toAbsolutePath.toString)).listFiles().toList.isEmpty) {
        Files.delete(path.getParent)
      }
    })
  }

  private def sortFolderNames(dirPath: String): util.TreeMap[LocalDate, String] = {

    val folderDateTimeMap = new util.TreeMap[LocalDate, String]()
    new io.File(dirPath).listFiles.filter(_.isDirectory).map(_.getName)
                        .map(folderName => folderDateTimeMap.put(LocalDate.parse(folderName, DateTimeFormatter.ofPattern("yyyy-MM-dd")), folderName)).toList;
    folderDateTimeMap;
  }

  private def runSparkJobToGenerateTopContentViewReport(spark: SparkSession, inputDataFrame: DataFrame, targetReportFolderPath: String, reportDate: String) = {
    val reportNameTopContent = String.format("%s_%s", "topContent_", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
    val reportNameDir = String.format("%s/%s", targetReportFolderPath, "topContent")
    val reportPath = String.format("%s/%s.%s", reportNameDir, reportNameTopContent, "csv")

    inputDataFrame.printSchema()
    inputDataFrame.createOrReplaceTempView("analyticsView");
    val sparkQueryToExecute = String.format("select SUBSTRING_INDEX(context.page.title, '-', 1) as realtitle, sum(properties.video_length) as totalLengthOfVideoWatched  from analyticsView where event = 'Watched Video' and  DATE(timestamp) > date_add(DATE('%s'), -7) and DATE(timestamp) <= DATE('%s') group by realtitle sort by totalLengthOfVideoWatched desc",reportDate, reportDate);
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
val inputDataFrame = spark.read.json("/Users/priyadarshanp/Documents/DEBootCamp/files/Processed/allFiles/5dd60ffd-4681-33a6-b847-fb32745599bc.json.gz")
inputDataFrame.createOrReplaceTempView("analyticsView")
spark.sql("select distinct DATE(timestamp) from analyticsView sort by DATE(timestamp) asc").show(200)
 */