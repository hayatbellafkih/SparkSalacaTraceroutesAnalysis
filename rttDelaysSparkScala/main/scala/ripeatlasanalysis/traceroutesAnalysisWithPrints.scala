package ripeatlasanalysis

import org.joda.time.{ DateTime, Period, DateTimeZone, LocalDateTime }
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import com.typesafe.config._
import java.nio.file.Paths
import Tools._
import org.apache.spark.sql.Encoders
import classes._
import ripeatlasanalysis.Tools.generateDateSample

object traceroutesAnalysisWithPrints {

  // load configuration
  val config = loadConfiguration()

  // create the name of log file
  val ExecutionMessage = createLogMessageFilePath(config.getString("prefixOutputLog"))

  // main function
  def main(args: Array[String]): Unit = {

    DateTimeZone.setDefault(DateTimeZone.UTC);
    val startTimeMillis = System.currentTimeMillis()

    linkDelayAnalysis(args, config)

    // compute the time of execution of the program
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

    appendMessage("Total time is : %d secondes".format(durationSeconds), ExecutionMessage)

  }

  def linkDelayAnalysis(args: Array[String], config: Config): Unit = {

    // Set the time zone as UTC
    implicit val localDateOrdering: Ordering[java.time.LocalDateTime] = Ordering.by(_.toEpochSecond(java.time.ZoneOffset.UTC))

    // Spark configuration : create configuration
    val conf = new SparkConf().setAppName("RTT delays analysis").setMaster("local")

    // Find the traceroutes in the given folder
    val dataPath = config.getString("fileFolder")

    if (!checkFileExist(dataPath)) {
      System.exit(0)
    }

    // Spark configuration : create spark session
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    try {
      val start = args(0).toInt
      val end = args(1).toInt
      val timewindow = args(2).toInt

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

      //Group each traceroute by the bin that they belong in
      // If one traceroute does not belongs in any bin, then by default it belongs to the bin 0
      val tracerouteAndPeriodRdd = rawtraceroutes.rdd.map(traceroute => TracerouteWithTimewindow(traceroute, findTimeWindowOfTraceroute(traceroute, rangeDatesTimewindows)))

      // Filter non concerned traceroutes from the given period; those having period = 0
      appendMessage("INFO : Remove non concerned traceroutes", ExecutionMessage)
      val onlyConcernedTraceoutes = tracerouteAndPeriodRdd.filter(_.period != 0)

      // Group the traceroute by bin
      appendMessage("INFO : Group traceroutes by bin ", ExecutionMessage)
      val groupedTraceroutesByPeriod = onlyConcernedTraceoutes.groupBy(_.period)
      val traceroutesPerPeriod = groupedTraceroutesByPeriod.map(f => TraceroutesPerPeriod(f._2.map(f => f.traceroute).toSeq, f._1))

      val traceroutesPerPeriodStr = traceroutesPerPeriod.map(traceroutesPerPeriodToString)

      appendMessage(traceroutesPerPeriodStr.collect().mkString("|||"), ExecutionMessage)
      val allLinksRttDiffsPeriods = traceroutesPerPeriod.map(f => linksInference(spark, f))

      // TODO delete
      println(allLinksRttDiffsPeriods.collect().mkString("\n"))

      val collectedRTTDiff = allLinksRttDiffsPeriods.collect().toSeq.flatten

      /* Grouping the data by link */
      val finalResult = collectedRTTDiff.groupBy(_.link)
      val finalRawRttDiff = finalResult.map(f => ResumedLink(f._1, (f._2.map(_.probes)).flatten, (f._2.map(_.rttDiffs)).flatten, (f._2.map(_.bins)).flatten))

      /*
      // get only the links having mor than a given rtt diff
      val allLinks = finalRawRttDiff.filter(p => p.rtts.size > args(3).toInt).mkString("|")
      appendMessage(allLinks, ExecutionMessage)
      */

      appendMessage(finalRawRttDiff.mkString("   link : "), ExecutionMessage)

      appendMessage("INFO : end of the preparation", ExecutionMessage)
      appendMessage("INFO : starting the alarms detection", ExecutionMessage)

      val rawDataLinkFiltred = spark.sparkContext.parallelize(finalRawRttDiff.toSeq)
        .map(p => listAlarms(spark, p, timewindow, rangeDates))

      val dateFormatter = new SimpleDateFormat("dd-MM-yyyy_hh-mm")
      val submittedDateConvert = new Date()
      val submittedAt = dateFormatter.format(submittedDateConvert)

      rawDataLinkFiltred.saveAsTextFile(submittedAt)

    } catch {
      case ex: ArrayIndexOutOfBoundsException => {
        appendMessage("Less than needed params ... ", ExecutionMessage)
      }
    }

  }

}