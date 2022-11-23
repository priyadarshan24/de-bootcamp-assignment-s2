
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.{io, util}

object SimpleApp {
  def main(args: Array[String]) {
    val toBeProcessedFolderLocation = "/Users/priyadarshanp/Documents/DEBootCamp/files/toBeProcessed"
    val processedFolderLocation = "/Users/priyadarshanp/Documents/DEBootCamp/files/Processed"
    val reportFolderLocation = "/Users/priyadarshanp/Documents/DEBootCamp/files/Reports"
    val sortedFolderMap = sortFolderNames("/Users/priyadarshanp/Documents/DEBootCamp/files/toBeProcessed");
    val allDataRepoFolder = "/Users/priyadarshanp/Documents/DEBootCamp/files/allFileRepo"

    sortedFolderMap.keySet().forEach(_ => {
      val folderName = sortedFolderMap.get(_);
      val processedTargetFolder = Files.createDirectory(Paths.get(String.format("%s/%s", processedFolderLocation, folderName)));
      val toBeProcessedSourceFolder = Files.createDirectory(Paths.get(String.format("%s/%s", toBeProcessedFolderLocation, folderName)));

      toBeProcessedSourceFolder.toFile.listFiles().foreach(_ => {
        
      })



      Files.move(toBeProcessedSourceFolderPath, processedTargetFolder.toAbsolutePath, StandardCopyOption.ATOMIC_MOVE)
      Files.


    });

      println("Printing sortedFolderMap:" +sortedFolderMap);
  }

  private def sortFolderNames(dirPath:String): util.TreeMap[LocalDate, String] = {

    val folderDateTimeMap = new util.TreeMap[LocalDate, String]()
    val list1 = new File(dirPath).listFiles.filter(_.isDirectory).map(_.getName).toList;

    val list = new io.File(dirPath).listFiles.filter(_.isDirectory).map(_.getName)
      .map(folderName => folderDateTimeMap.put(LocalDate.parse(folderName, DateTimeFormatter.ofPattern("yyyy-MM-dd")), folderName)).toList;

    folderDateTimeMap;
  }
}



/*val conf = new SparkConf().setMaster("spark://Priyadarshans-MacBook-Pro-2.local:7077")
    .setAppName("Simple Application").setJars(Array("/Users/priyadarshanp/Documents/DEBootCamp/jars/postgresql-42.5.0.jar")).set("spark.jars", "/Users/priyadarshanp/Documents/DEBootCamp/jars/postgresql-42.5.0.jar")

//    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "spark://Priyadarshans-MacBook-Pro-2.local:707711").getOrCreate()
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
    val googleAnalyticsData = spark.read.json("/Users/priyadarshanp/Documents/DEBootCamp/files/toBeProcessed/*.gz");
    googleAnalyticsData.printSchema()
    googleAnalyticsData.createOrReplaceTempView("analyticsView");


    var popularContentByTileBasisVideoLengthWatched = spark.sql("select SUBSTRING_INDEX(context.page.title, '-', 1) as realtitle, sum(properties.video_length) as totalLengthOfVideoWatched  from analyticsView where event = 'Watched Video' group by realtitle sort by totalLengthOfVideoWatched desc").show(10, false)



    //Answers the Query 1 >> Total no of distinct users of the system basis data shared >> 10000
   spark.sql("Select count(distinct userId) from analyticsView").show()

    spark.sql("select distinct timestamp from analyticsView").show()

    //println("Printing no of users>>>>::" + numberOfUsers.toString)*/

  }


}

/*


   //Answers the Query 2 >> Total number of record/events basis which business query answers are been found >> 6670255
   val totalNumberOfRecords = spark.sql("Select count(distinct *) from analyticsView").show()

   //Answers the Query 1>> Distribution of event Types
   /** +---------------+--------+
    *  |          event|count(1)|
    *  +---------------+--------+
    *  |    Viewed Page| 2098003|
    *  |Content Clicked| 1142911|
    *  |  Watched Video| 1143215|
    *  |   Ad Requested| 1142911|
    *  |  Started Video| 1143215|
    *  +---------------+--------+
    * */
   val eventType = spark.sql("Select event,count(*) from analyticsView group by event").show()

   //Answers the Query 3 >> Which Platform users prefers to watch content  >> Android
   /**
    *
    *  +---------+---------+
    *  | platform|noOfUsers|
    *  +---------+---------+
    *  |  ANDROID|     5014|
    *  |      WEB|     2440|
    *  |      IOS|     1661|
    *  |      IOS|     1661|
    *  |FIRESTICK|      885|
    *  +---------+---------+
    */
   val preferredPlatform = spark.sql("select context.traits.platform, count(distinct userid) as noOfUsers from analyticsView where event = 'Watched Video' group by context.traits.platform sort by noOfUsers desc").show(100)


   //Answers the Query 4 >> Most Popular Content by Title across all the users basis no of views
   /**
    * +-----------------+--------------+
    *  |realtitle        |totalNoOfViews|
    *  +-----------------+--------------+
    *  |Curly            |17986         |
    *  |Spinach          |17979         |
    *  |Kale             |17934         |
    *  |English Cucumber |9262          |
    *  |Escarole         |9204          |
    *  |Candle Corn      |9189          |
    *  |Parsnip          |9178          |
    *  |Nopales          |9160          |
    *  |Lotus Seed       |9148          |
    *  |Beet Greens      |9121          |
    *  +-----------------+--------------+
    */
   var popularContentByTitle = spark.sql("select SUBSTRING_INDEX(context.page.title, '-', 1) as realtitle, count(*) as totalNoOfViews  from analyticsView where event = 'Watched Video' group by realtitle sort by totalNoOfViews desc").show(10, false)

   //Answers the Query 4 >> Most Popular Content by Title across all the users total Length of Video Watched
   /**
    * Popular Content By title basis total Length of Video Watched
    *
    * +-----------------+-------------------------+
    *  |realtitle        |totalLengthOfVideoWatched|
    *  +-----------------+-------------------------+
    *  |Spinach          |32472158                 |
    *  |Curly            |32450450                 |
    *  |Kale             |31902867                 |
    *  |Beet Greens      |16731536                 |
    *  |Candle Corn      |16676805                 |
    *  |Escarole         |16627345                 |
    *  |Zucchini         |16584718                 |
    *  |Capers           |16583310                 |
    *  |Parsnip          |16570268                 |
    *  |English Cucumber |16539934                 |
    *  +-----------------+-------------------------+
    */
   var popularContentByTileBasisVideoLengthWatched = spark.sql("select SUBSTRING_INDEX(context.page.title, '-', 1) as realtitle, sum(properties.video_length) as totalLengthOfVideoWatched  from analyticsView where event = 'Watched Video' group by realtitle sort by totalLengthOfVideoWatched desc").show(10, false)


   /* Answers the Query 5 >> Does the number of ads displayed affect how much content the user sees?
       Users are evently distributed when it comes to subscription status
       +--------+-------------------+
       |count(1)|subscription_status|
       +--------+-------------------+
       |     124|               FREE|
       |     124|            PREMIUM|
       |     124|           FREEMIUM|
       +--------+-------------------+

       Logic used to find answer is - find total video length watched subscription wise and title wise
       From this, average viewing time subscription wise and title wise was found
       Comparison was done to check no of instancs title wise that average viewing time per title  for premium was more than for free and free premium
       It was found that there are 30% cases where premium content watching hours per user is more than Free subscription
       This mean having ads have no impact on content viewed

                   +--------------------+-------+--------+------------------+------------------+
|           realtitle| source|  target|         sourceAVT|         targetAVT|
+--------------------+-------+--------+------------------+------------------+
|              Onion |PREMIUM|FREEMIUM| 2856.888381532217| 2811.866344605475|
|     Hearts of Palm |PREMIUM|FREEMIUM| 2776.883103624298| 2725.946688206785|
|               Leaf |PREMIUM|    FREE| 2752.433107756377|2739.9403973509934|
|           Pak Choy |PREMIUM|    FREE|2799.6637214137213| 2767.308510638298|
|          Rutabagat |PREMIUM|    FREE| 2804.087531806616|2794.8522727272725|
|            Parsley |PREMIUM|    FREE| 2849.653050672182|2787.7123572170303|
|           Red Leaf |PREMIUM|    FREE| 2875.707910750507| 2808.929833417466|
|Celery Root/Celer...|PREMIUM|    FREE| 2861.622199592668| 2783.337203166227|
|   English Cucumber |PREMIUM|FREEMIUM|   2850.5780403742| 2781.932069510269|
|             Tomato |PREMIUM|    FREE| 2839.848469387755| 2805.500518672199|
|Celery Root/Celer...|PREMIUM|FREEMIUM| 2861.622199592668|2782.6137203166227|
|            Chayote |PREMIUM|FREEMIUM|2816.9933875890133| 2773.239809422975|
|             Potato |PREMIUM|FREEMIUM|2803.8813813813813| 2758.258150721539|
|             Rapini |PREMIUM|FREEMIUM|2870.0183673469387|2729.8946808510636|
|             Garlic |PREMIUM|    FREE|2787.0544147843943|2766.9441312147615|
|            Spinach |PREMIUM|FREEMIUM|4014.2781981981984| 3950.327306967985|
|              Curly |PREMIUM|FREEMIUM| 3979.874283667622|3887.2134032197678|
|             Squash |PREMIUM|    FREE|2819.0416016640665| 2780.843047034765|
|         Lotus Root |PREMIUM|FREEMIUM| 2866.468106479156| 2818.731681034483|
|              Maize |PREMIUM|    FREE|2806.9158266129034|2798.9539098912483|
|         Butterhead |PREMIUM|FREEMIUM|2792.8333333333335|2711.8996262680193|
|      Bamboo Shoots |PREMIUM|FREEMIUM|  2803.08751941999|  2802.93074054342|
|            Parsley |PREMIUM|FREEMIUM| 2849.653050672182|2771.2520325203254|
|           Eggplant |PREMIUM|FREEMIUM| 2854.960143076137|2820.6874666666668|
|Green Onions/Scal...|PREMIUM|FREEMIUM| 2783.310824742268| 2733.932121859968|
|           Shallots |PREMIUM|FREEMIUM|2756.9429735234216|2722.1934976402727|
|       Bean Sprouts |PREMIUM|    FREE| 2877.813471502591|2836.3165542293723|
|Bitter Melon/Bitt...|PREMIUM|    FREE| 2890.861522738886|2814.6201671891326|
|           Lacinato |PREMIUM|    FREE|2853.1543116490166|2771.1627420198847|
|           Lacinato |PREMIUM|FREEMIUM|2853.1543116490166|2800.1695005313495|
|       Napa Cabbage |PREMIUM|    FREE|2887.1249367728883| 2794.974580579563|
|        Cauliflower |PREMIUM|    FREE| 2851.185204345577| 2779.550590652286|
|     Belgian Endive |PREMIUM|FREEMIUM|2842.7436953165206|2820.0570971184634|
|        Cauliflower |PREMIUM|FREEMIUM| 2851.185204345577|2771.6652360515022|
|     Water Chestnut |PREMIUM|    FREE| 2772.879493670886| 2679.262125902993|
|        Candle Corn |PREMIUM|    FREE|2886.1185410334347|2863.8120805369126|
|           Bok Choi |PREMIUM|    FREE|2825.8915167095115|2763.5384207737147|
|   Brussels Sprouts |PREMIUM|FREEMIUM|2776.5612403100777| 2734.181865018032|
|            Spinach |PREMIUM|    FREE|4014.2781981981984| 3949.168608885652|
|              Onion |PREMIUM|    FREE| 2856.888381532217|2817.7016828148903|
|             Celery |PREMIUM|    FREE|2780.5410652065702|2767.9604462474645|
|         Green Leaf |PREMIUM|    FREE|2854.6779575328615| 2794.754966887417|
|       Winter Melon |PREMIUM|    FREE|2822.5347892331133|2815.6631962519523|
|              Beans |PREMIUM|    FREE| 2840.387550200803|2721.3279270146986|
|              Olive |PREMIUM|FREEMIUM|2849.8793192367198|2777.0795155344917|
|         Butterhead |PREMIUM|    FREE|2792.8333333333335|2777.4185320145757|
|     Belgian Endive |PREMIUM|    FREE|2842.7436953165206|2798.4472208057114|
|   Dandelion Greens |PREMIUM|    FREE| 2822.233625580196|2737.1430035879034|
|             Garlic |PREMIUM|FREEMIUM|2787.0544147843943| 2744.550901378579|
|               Peas |PREMIUM|FREEMIUM|2812.5687276443537| 2797.282458929518|
|          Artichoke |PREMIUM|    FREE|2763.9168778509884| 2721.089709762533|
|               Taro |PREMIUM|    FREE|2830.1016607951688|2820.2503888024885|
|            Nopales |PREMIUM|    FREE|2870.0325534079348| 2774.305171530978|
|Jerusalem Artichoke |PREMIUM|    FREE| 2855.991799077396| 2709.530883103624|
|    Amaranth Leaves |PREMIUM|FREEMIUM|2827.0472036942024|2783.5460878885315|
|          Snow peas |PREMIUM|FREEMIUM|2804.3658286300665| 2788.817943336831|
|    Amaranth Leaves |PREMIUM|    FREE|2827.0472036942024| 2767.168426344897|
|    Chinese Spinach |PREMIUM|FREEMIUM|2844.8593988792663| 2781.505801687764|
|              Maize |PREMIUM|FREEMIUM|2806.9158266129034|      2768.7484375|
|            Edamame |PREMIUM|    FREE|2801.1326164874554| 2750.614513638703|
|        Bell Pepper |PREMIUM|    FREE|2839.8433062880326|2778.5512367491165|
|               Kale |PREMIUM|FREEMIUM|3932.9480144404333|3830.1023272995935|
|             Turnip |PREMIUM|FREEMIUM|2783.1434511434513|2767.7076205287713|
|         Snap Beans |PREMIUM|    FREE|2829.5615505500264|2808.5074550128534|
|   English Cucumber |PREMIUM|    FREE|   2850.5780403742|2781.0381485249236|
|            Cassava |PREMIUM|FREEMIUM| 2860.485699693565|2805.7059136920616|
|     Mustard Greens |PREMIUM|    FREE|2787.5304878048782| 2709.554353426069|
|      Bamboo Shoots |PREMIUM|    FREE|  2803.08751941999|2724.4889687018986|
|   Dandelion Greens |PREMIUM|FREEMIUM| 2822.233625580196|2811.7708674304417|
|         Snap Beans |PREMIUM|FREEMIUM|2829.5615505500264| 2790.873821989529|
|    Kohlrabi Greens |PREMIUM|FREEMIUM|2785.8383631713555|2746.6682027649767|
|       Napa Cabbage |PREMIUM|FREEMIUM|2887.1249367728883|2773.9671087533156|
|      Water Spinach |PREMIUM|    FREE| 2821.981772151899|2788.9502819067143|
|       String Beans |PREMIUM|FREEMIUM|2788.4414278531926|2752.1928460342147|
|          Snow peas |PREMIUM|    FREE|2804.3658286300665|2788.9402286902287|
|          Mushrooms |PREMIUM|    FREE|  2822.85417721519|2761.4673694779117|
|             Ginger |PREMIUM|    FREE|2870.0868676545733|2862.0867293625915|
|    Elephant Garlic |PREMIUM|FREEMIUM| 2750.676544450025|2744.9032597266037|
|     Collard Greens |PREMIUM|    FREE| 2778.845620979713|2739.0587341772152|
|            Cassava |PREMIUM|    FREE| 2860.485699693565|2769.4979296066253|
|        Green Beans |PREMIUM|    FREE|2756.4185684647305| 2717.967938931298|
|       Bean Sprouts |PREMIUM|FREEMIUM| 2877.813471502591|  2717.09506302521|
|          Aubergine |PREMIUM|    FREE|2820.6299800796814|2754.5688956433637|
|             Radish |PREMIUM|FREEMIUM| 2816.206753246753| 2803.878242456326|
|               Taro |PREMIUM|FREEMIUM|2830.1016607951688| 2764.331717824448|
|              Beans |PREMIUM|FREEMIUM| 2840.387550200803| 2830.176249328318|
|          Sunchokes |PREMIUM|    FREE|2832.7202230106436| 2696.161741835148|
|            Edamame |PREMIUM|FREEMIUM|2801.1326164874554|2745.4804852320676|
|            Cabbage |PREMIUM|    FREE| 2885.994396332145|2798.6632390745503|
|           Eggplant |PREMIUM|    FREE| 2854.960143076137|2721.2278806584363|
|             Tomato |PREMIUM|FREEMIUM| 2839.848469387755|2762.4155200880573|
|     Mustard Greens |PREMIUM|FREEMIUM|2787.5304878048782|2754.8133262823903|
|           Purslane |PREMIUM|    FREE|2804.0861268695203|2797.0525773195877|
|             Squash |PREMIUM|FREEMIUM|2819.0416016640665|2743.2021716649433|
|            Nopales |PREMIUM|FREEMIUM|2870.0325534079348|2797.8158581116327|
|             Celery |PREMIUM|FREEMIUM|2780.5410652065702|2777.2578616352203|
|            Parsnip |PREMIUM|FREEMIUM| 2878.229566094854|  2812.01758124667|
|             Carrot |PREMIUM|    FREE|2848.2763358778625|  2743.42659137577|
|           Calabash |PREMIUM|FREEMIUM|2804.1594129554655|2796.2278012684988|
|            Celtuce |PREMIUM|FREEMIUM|2811.6664951106536|2714.4898504273506|
|               Beet |PREMIUM|FREEMIUM|2857.4409650924026|2841.9176285414483|
|            Parsnip |PREMIUM|    FREE| 2878.229566094854| 2853.656792645557|
|            Cabbage |PREMIUM|FREEMIUM| 2885.994396332145| 2798.264859228363|
|               Peas |PREMIUM|    FREE|2812.5687276443537| 2668.898969072165|
|        Bell Pepper |PREMIUM|FREEMIUM|2839.8433062880326| 2795.871928907475|
|       Winter Melon |PREMIUM|FREEMIUM|2822.5347892331133|2732.1838394793926|
|          Mushrooms |PREMIUM|FREEMIUM|  2822.85417721519| 2784.863146551724|
|  Green, Red, Savoy |PREMIUM|    FREE|2810.7472868217055|2764.6987212276213|
|             Ginger |PREMIUM|FREEMIUM|2870.0868676545733| 2802.012765957447|
|               Beet |PREMIUM|    FREE|2857.4409650924026| 2686.722138174483|
|             Frisee |PREMIUM|    FREE| 2867.108415074858| 2783.062951496388|
|               Corn |PREMIUM|FREEMIUM| 2803.244534824606|2729.9148598625065|
| Pickling Cucumbers |PREMIUM|    FREE| 2819.862623120788| 2772.446511627907|
|    Chinese Spinach |PREMIUM|    FREE|2844.8593988792663| 2784.083756345178|
|             Rapini |PREMIUM|    FREE|2870.0183673469387|  2863.32254697286|
|Bitter Melon/Bitt...|PREMIUM|FREEMIUM| 2890.861522738886|2745.1667554608416|
|          Wax Beans |PREMIUM|    FREE| 2798.744939271255|2762.6773869346735|
|      Daikon Radish |PREMIUM|    FREE| 2797.829306722689|2796.7376288659793|
|              Olive |PREMIUM|    FREE|2849.8793192367198|2820.0295767465577|
|           Pak Choy |PREMIUM|FREEMIUM|2799.6637214137213|2793.8778751369114|
|               Kale |PREMIUM|    FREE|3932.9480144404333|3865.0613875771887|
|           Escarole |PREMIUM|FREEMIUM|2809.2179809141135| 2802.057550158395|
|              Curly |PREMIUM|    FREE| 3979.874283667622|3978.1750181554103|
|         Fiddlehead |PREMIUM|FREEMIUM| 2786.647206844489|2772.1862745098038|
|         Fiddlehead |PREMIUM|    FREE| 2786.647206844489|2786.1623711340208|
|               Gobo |PREMIUM|    FREE|2844.4584178498985|2755.6561876247506|
|           Red Leaf |PREMIUM|FREEMIUM| 2875.707910750507|2748.2642626480088|
|             Frisee |PREMIUM|FREEMIUM| 2867.108415074858| 2808.827012987013|
|         Lotus Root |PREMIUM|    FREE| 2866.468106479156| 2725.339937434828|
|               Gobo |PREMIUM|FREEMIUM|2844.4584178498985|2728.4921066956995|
|             Greens |PREMIUM|FREEMIUM|2805.6348258706466| 2747.095263157895|
|               Yuca |PREMIUM|FREEMIUM|2804.1158163265304| 2792.449420442571|
|         Green Leaf |PREMIUM|FREEMIUM|2854.6779575328615|2774.2454545454543|
|             Jícama |PREMIUM|FREEMIUM| 2839.531914893617| 2754.492920818039|
|             Fennel |PREMIUM|FREEMIUM|          2798.104|2780.9664864864867|
|    Kohlrabi Greens |PREMIUM|    FREE|2785.8383631713555|2728.9752194114612|
|             Endive |PREMIUM|    FREE|2820.3211340206185| 2749.087894736842|
|          Aubergine |PREMIUM|FREEMIUM|2820.6299800796814|2798.0712401055407|
|             Capers |PREMIUM|FREEMIUM| 2843.043259557344| 2842.454350161117|
|        Candle Corn |PREMIUM|FREEMIUM|2886.1185410334347| 2847.171383647799|
|          Baby Corn |PREMIUM|    FREE| 2787.431234256927| 2778.522633744856|
|               Corn |PREMIUM|    FREE| 2803.244534824606| 2735.480040941658|
+--------------------+-------+--------+------------------+------------------+
    */
   //SubAnswers >> Find no of users watching a title by subscription status and total videolength watched
   var contentViewingStatsBySubscriptionStatus = spark.sql("select SUBSTRING_INDEX(context.page.title, '-', 1) as realtitle, event, context.traits.subscription_status, count(distinct userid) as totalusers, sum(properties.video_length) as totalVideoLength from analyticsView where event in ('Watched Video') group by realtitle, context.traits.subscription_status, event sort by realtitle desc")

   contentViewingStatsBySubscriptionStatus.createOrReplaceTempView("contentViewingStatsBySubscriptionStatus");

   var perUserView = spark.sql("select *, totalVideoLength/totalusers as averageViewingTime from contentViewingStatsBySubscriptionStatus sort by realtitle desc, averageViewingTime desc");
   perUserView.createOrReplaceTempView("perUserView");
   val selfJoinDF = perUserView.as("v1").join(perUserView.as("v2"), col("v1.realtitle") === col("v2.realtitle"), "inner");
   val selfJoinDFRenamed = selfJoinDF.select(col("v1.realtitle"), col("v1.subscription_status").as("source"), col("v2.subscription_status").as("target"), col("v1.averageViewingTime").as("sourceAVT"), col("v2.averageViewingTime").as("targetAVT"));
   selfJoinDFRenamed.createOrReplaceTempView("selfJoinDF");
   spark.sql("select count(*) from selfJoinDF where source = 'PREMIUM' and target != 'PREMIUM' and sourceAVT > targetAVT").show(1500)


   /**
    * Answer Type 1: Different plans for mobiles subscription vs web
    * Mobile seems to be preferred platform for content consumption both in Free, Freemium and Paid. Lot of users
    * can be converted into paying customers by having a new mobile subscription plan with reduced cost
    *
    * +-------------------+---------+----------------------------+
    *  |subscription_status| platform|((count(1) * 100) / 1143215)|
    *  +-------------------+---------+----------------------------+
    *  |               FREE|  ANDROID|          16.947380851370916|
    *  |               FREE|FIRESTICK|           2.763522172119855|
    *  |               FREE|      IOS|           5.572530101511964|
    *  |               FREE|      WEB|           8.247005156510367|
    *  |           FREEMIUM|  ANDROID|          16.602476349593033|
    *  |           FREEMIUM|FIRESTICK|          2.8756620583179893|
    *  |           FREEMIUM|      IOS|            5.37624156436016|
    *  |           FREEMIUM|      WEB|            7.69522793175387|
    *  |            PREMIUM|  ANDROID|          16.634141434463334|
    *  |            PREMIUM|FIRESTICK|           3.253806151948671|
    *  |            PREMIUM|      IOS|            5.64871874494299|
    *  |            PREMIUM|      WEB|           8.383287483106852|
    *  +-------------------+---------+----------------------------+
    *
    *
    */
   spark.sql("select context.traits.platform, count(*)*100/1143215  from analyticsView where event = 'Watched Video' group by context.traits.platform").show
   spark.sql("select context.traits.subscription_status, context.traits.platform, count(*)*100/1143215  from analyticsView where event = 'Watched Video' group by context.traits.subscription_status, context.traits.platform sort by context.traits.subscription_status, context.traits.platform").show

   /**
    * Another strategy would be to have HD vs SD vs Full HD basis different pricing strategy
    * Basis below data it seems content is been requested in either Full HD/HD but content is been served only in SD
    * A cheaper pricing plan can be created for SD streaming - for areas of intermittent connectivity or poor internet issue
    *
    * Assumption is: requestedTag resolution indicates what content Type is requested SD/HD/FHD
    * playbackTag signifies what is actually streamed SD/HD/FHD
    *    +--------+
         |count(1)|
         +--------+
         | 1143215|
         +--------+
    */
   spark.sql("select count(*) from analyticsView where (contains(properties.requested_tag,'\"resolution\":\"fhd\"') or contains(properties.requested_tag,'\"resolution\":\"hd\"')) and contains(properties.playback_tag,'\"resolution\":\"sd\"')  and event = 'Watched Video'")*/


/** Parsing String as JSON Schema
*
*  val testData: DataFrame = spark.sql("select properties.playback_tag, properties.requested_tag, properties.client_capabilities from analyticsView where properties.playback_tag is not null")
   //https://sparkbyexamples.com/spark/spark-parse-json-from-text-file-string/

   /*
   {"audio_channel":"stereo","container":"fmp4","dynamic_range":"sdr","encryption":"widevine","ladder":"tv","package":"dash","resolution":"fhd","video_codec":"h264"}
    */
   import org.apache.spark.sql.types.{StringType, StructType};

   val playBackAndRequestedTagSchema = new StructType().add("audio_channel", StringType, true).add("container", StringType, true).add("dynamic_range", StringType, true).add("encryption", StringType, true).add("ladder", StringType, true).add("package", StringType, true).add("resolution", StringType, true).add("video_codec", StringType, true)

   val playBackTag = testData.withColumn("myJson", from_json(col("playback_tag"), playBackAndRequestedTagSchema)).select("myJson.*")
   playBackTag.createOrReplaceTempView("playBackTag");
   spark.sql("select * from playBackTag")

   val requestedTag = testData.withColumn("myJson", from_json(col("requested_tag"), playBackAndRequestedTagSchema)).select("myJson.*")
   requestedTag.createOrReplaceTempView("requestedTag");
   spark.sql("select * from requestedTag")

   spark.sql("select count(*) from analyticsView where contains(properties.playback_tag,'\"resolution\":\"hd\"')  and event = 'Watched Video'")

   spark.sql("select count(*) from analyticsView where contains(properties.requested_tag,'\"resolution\":\"hd\"')  and event = 'Watched Video'")
   /* val clientCapabilitiesTagSchema =
      new StructType().add("video_codec", ArrayType, true).add("containers", StringType, true).add("audio_channel", StringType, true).add("encryption", StringType, true).add("widevine_security_level", StringType, true).add("widevine_hdcp_version", StringType, true).add("ads", StringType, true).add("dvr", StringType, true).add("dynamicRange", StringType, true).add("ladder", StringType, true).add("package", StringType, true).add("resolution", StringType, true)
   */
*/
/**
 * Distinct timestamps >> Feb, Jan and March 1st week of Data
 *
 * +------------------+
|eventDateTimestamp|
+------------------+
|        2022-01-01|
|        2022-01-02|
|        2022-01-03|
|        2022-01-04|
|        2022-01-05|
|        2022-01-06|
|        2022-01-07|
|        2022-01-08|
|        2022-01-09|
|        2022-01-10|
|        2022-01-11|
|        2022-01-12|
|        2022-01-13|
|        2022-01-14|
|        2022-01-15|
|        2022-01-16|
|        2022-01-17|
|        2022-01-18|
|        2022-01-19|
|        2022-01-20|
|        2022-01-21|
|        2022-01-22|
|        2022-01-23|
|        2022-01-24|
|        2022-01-25|
|        2022-01-26|
|        2022-01-27|
|        2022-01-28|
|        2022-01-29|
|        2022-01-30|
|        2022-01-31|
|        2022-02-01|
|        2022-02-02|
|        2022-02-03|
|        2022-02-04|
|        2022-02-05|
|        2022-02-06|
|        2022-02-07|
|        2022-02-08|
|        2022-02-09|
|        2022-02-10|
|        2022-02-11|
|        2022-02-12|
|        2022-02-13|
|        2022-02-14|
|        2022-02-15|
|        2022-02-16|
|        2022-02-17|
|        2022-02-18|
|        2022-02-19|
|        2022-02-20|
|        2022-02-21|
|        2022-02-22|
|        2022-02-23|
|        2022-02-24|
|        2022-02-25|
|        2022-02-26|
|        2022-02-27|
|        2022-02-28|
|        2022-03-01|
|        2022-03-02|
+------------------+
 */
