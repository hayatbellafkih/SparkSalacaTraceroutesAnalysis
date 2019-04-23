package ripeatlasanalysis

//imports
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import classes._
import ripeatlasanalysis.Tools.generateDateSample
import Tools._
object rttDiff {

  val ExecutionMessage = createLogMessage()

  def main(args: Array[String]): Unit = {

    //val dataPath = "/home/hayat/ha"
    val dataPath = "file:///home//162558//spark//input"
    findMeanDistribution(args, dataPath)
  }

  def findMeanDistribution(args: Array[String], dataPath: String) {

    /* Input parameters
       *
       * start
       * end
       * timewindow
       */

    //val args= Array("1514769809",  "1514799809" ,"3600")
    try {
      val start = args(0).toInt
      val end = args(1).toInt
      val timewindow = args(2).toInt

      // Set the the time zone Configuration
      implicit val localDateOrdering: Ordering[java.time.LocalDateTime] = Ordering.by(_.toEpochSecond(java.time.ZoneOffset.UTC))

      // Spark configuration :  specify master type
      val conf = new SparkConf().setAppName("RTT delays analysis").setMaster("local")

      //Spark configuration :  create Spark session
      val spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()
      import spark.implicits._

      //Mapping of each Traceroute   record on the case class @see Class Traceroute
      appendMessage("INFO : Reading files in folder  " + dataPath, ExecutionMessage)
      val rawtraceroutes = spark.read
        .schema(Encoders.product[Traceroute].schema)
        .json(dataPath)
        .as[Traceroute]

      import spark.implicits._

      //Generate the start of all  bins : between start date and end date espaced by the timewindow
      val rangeDates = generateDateSample(start, end, timewindow)
      appendMessage("INFO : Find   all start of bins [start] :\n " + rangeDates.mkString("|"), ExecutionMessage)

      // Find all the bins start and end
      val rangeDatesTimewindows = rangeDates.map(f => (f, f + timewindow))
      appendMessage("INFO : Find   all bins  [(start, end)] :\n " + rangeDatesTimewindows.mkString("|"), ExecutionMessage)

      //Count the number of read traceroutes in the given input files
      val totalTraceroutes = rawtraceroutes.rdd.count()
      appendMessage("INFO : Total traceroures in input files is " + totalTraceroutes.toString(), ExecutionMessage)

      /*Group each traceroute by the bin that they belong in */
      /* If one traceroute does not belongs in any bin, then by default it belongs to the bin 0*/
      val tracerouteAndPeriodRdd = rawtraceroutes.rdd.map(traceroute => TracerouteWithTimewindow(traceroute, findTimeWindowOfTraceroute(traceroute, rangeDatesTimewindows)))

      appendMessage("INFO : Gouping the traceroutes by period  and displaying only first 10 elements ", ExecutionMessage)
      //tracerouteAndPeriodRdd.saveAsTextFile("/home/hayat/ha/tracerouteswithperiod")
      //TODO change parameter to be dynamic

      // Filter non concerned traceroutes from the given period; those having period = 0
      appendMessage("INFO : Remove non concerned traceroutes", ExecutionMessage)
      val onlyConcernedTraceoutes = tracerouteAndPeriodRdd.filter(_.period != 0)

      // Group the traceroute by bin
      appendMessage("INFO : Group traceroutes by bin ", ExecutionMessage)
      val groupedTraceroutesByPeriod = onlyConcernedTraceoutes.groupBy(_.period)
      val traceroutesPerPeriod = groupedTraceroutesByPeriod.map(f => TraceroutesPerPeriod(f._2.map(f => f.traceroute).toSeq, f._1))

      val allLinksRttDiffsPeriods = traceroutesPerPeriod.map(f => linksInference(spark, f))
      val collectedRTTDiff = allLinksRttDiffsPeriods.collect().toSeq.flatten

      //allLinksRttDiffsPeriods.toDF().write.save("/home/hayat/ha/csv_output.csv")
      //z. dataFrame.write.format("com.databricks.spark.csv").save("myFile.csv")

      /* Grouping the data by link */
      val finalResult = collectedRTTDiff.groupBy(_.link)
      val finalRawRttDiff = finalResult.map(f => ResumedLink(f._1, (f._2.map(_.probes)).flatten, (f._2.map(_.rttDiffs)).flatten, (f._2.map(_.bins)).flatten))

     val allLinks =finalRawRttDiff.filter(p => p.rttDiffs.size> args(3).toInt).mkString("|")
     appendMessage(allLinks, ExecutionMessage)
     
     //val (startValues,counts) = allLinks. 
       
       //lifeExpectancyDF.select("LifeExp").map(value => value.getDouble(0)).rdd.histogram(5)
     

    } catch {
      case ex: ArrayIndexOutOfBoundsException => {
        appendMessage("Less than needed params ... ", ExecutionMessage)
      }
    }

  }

}