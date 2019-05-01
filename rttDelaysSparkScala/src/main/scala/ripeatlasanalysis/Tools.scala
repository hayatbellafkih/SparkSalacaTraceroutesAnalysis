package ripeatlasanalysis

import java.io.{ FileWriter }
import com.typesafe.config._
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SparkSession
import classes._
import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.apache.spark.mllib.rdd.RDDFunctions
object Tools {

  /**
   * Generate the timewindow start and end time
   */
  def generateDateSample(start: Int, end: Int, timewindow: Int): Seq[Int] = {
    Range(start, end, timewindow).toSeq
  }

  /**
   * Append the <i>message</i> to the log <i>file</i>
   * @author hayat
   * @param message
   * @return
   */
  def appendMessage(message: String, absolutefilePath: String) {

    val fw = new FileWriter(absolutefilePath, true);
    fw.write(message + "\n");
    fw.close()

  }

  def checkTracerouteTimewindows(traceroute: Traceroute, start: Int, end: Int): Boolean = {
    if (traceroute.timestamp >= start && traceroute.timestamp < end) {
      true
    } else
      false
  }

  def findTimeWindowOfTraceroute(traceroute: Traceroute, rangeDatesTimewindows: Seq[(Int, Int)]): Int = {

    val d = rangeDatesTimewindows.filter(p => checkTracerouteTimewindows(traceroute, p._1, p._2))
    if (d.size == 1) {
      d(0)._1
    } else {
      0
    }
  }

  /**
   * create  the Log file  having name <i>dd-MM-yyyy_hh-mm_mean_distribution.txt</i>
   * This file is created in the current directory where the .jar file is called
   * @author hayat
   * @return the name of the file
   */

  def createLogMessage(): String = {

    val dateFormatter = new SimpleDateFormat("dd-MM-yyyy_hh-mm")
    var submittedDateConvert = new Date()
    return dateFormatter.format(submittedDateConvert) + "_mean_distribution.txt"

  }

  /**
   *
   * create  the Log file  having name that terminate by the given  suffix
   *
   * This file is created in the current directory where the .jar file is called
   * @author hayat
   * @return the name of the file
   */
  def createLogMessageFilePath(suffix: String): String = {

    val dateFormatter = new SimpleDateFormat("dd-MM-yyyy_hh-mm")
    var submittedDateConvert = new Date()
    return dateFormatter.format(submittedDateConvert) + suffix

  }

  /**
   *  Check a Traceroute by checking each Signal of each Hop  of the given Traceroute  object
   *
   *  @return Traceroute
   */
  def removeNegative(traceroute: Traceroute): Traceroute = {

    val outerList = traceroute.result
    for (temp <- outerList) {

      val hops = temp.result
      val newinnerList = hops.filter(checkSignal(_))
      temp.result = newinnerList
    }
    traceroute
  }

  /**
   * @author hayat
   *
   */
  def linksInference(spark: SparkSession, rawtraceroutes: TraceroutesPerPeriod): Seq[ResumedLink] = {

    //Filter failed traceroutes ...
    val notFailedTraceroutes = rawtraceroutes.traceroutes.filter(t => t.result(0).result != null)

    //Remove invalid data  in hops
    val cleanedTraceroutes = notFailedTraceroutes.map(t => removeNegative(t))

    //Compute median by hop
    val tracerouteMedianByHop = cleanedTraceroutes.map(t => computeMedianRTTByhop(t))

    //Find links in a traceroute

    val tracerouteLinks = tracerouteMedianByHop.map(t => findLinksByTraceroute(spark, t))

    //Create a set of DetailedLink objects for every traceroute
    val detailedLinks = tracerouteLinks.map(resumeLinksTraceroute)

    //Flatten the list of lists to have one liste of DetailedLink objects
    val allDetailedLinks = detailedLinks.flatten

    //Sort the links
    val sortAllDetailedLinks = allDetailedLinks.map(l => sortLinks(l))

    //Merge the links from all traceroutes in the current bin
    val mergedLinks = sortAllDetailedLinks.groupBy(_.link)

    //Resume the link
    val resumeData = mergedLinks.map(f => ResumedLink(f._1, f._2.map(_.probe), f._2.map(_.rttDiff), generateDatesSample(f._2.size, rawtraceroutes.timeWindow)))

    resumeData.toSeq
  }

  /**
   * Find all links for a given traceroute
   */
  def findLinksByTraceroute(spark: SparkSession, traceroute: MedianByHopTraceroute): LinksTraceroute = {
    val hops = traceroute.result
    val size = hops.size
    val s = hops.zipWithIndex
    val z = s.map {
      case (element, index) =>
        if (index + 1 < size) {
          findAllLinks(hops(index + 1), element)
        } else {
          null
        }
    }
    return new LinksTraceroute(traceroute.dst_name, traceroute.from, traceroute.prb_id, traceroute.msm_id, traceroute.timestamp, z.filter(p => p != null).flatten)
  }

  /**
   * Find the link (s) between consecutives routers
   */
  def findAllLinks(firstRouter: PreparedHop, nextRouter: PreparedHop): Seq[Link] = {
    var links = Seq[Link]()

    for (currentRouter <- firstRouter.result) {
      for (prochainouter <- nextRouter.result) {
        val rttDiff = BigDecimal(currentRouter.medianRtt) - BigDecimal(prochainouter.medianRtt)
        links = links :+ Link(currentRouter.from, prochainouter.from, rttDiff.toDouble)
      }
    }
    // println(links.toString())
    return links
  }

  /**
   *
   * Check if the given Signal is valid by checking :
   *   the source IP address if is not private;
   *   the signal is valid (not contains the * )
   *   the rtt exists
   *   the rtt is not negative
   *
   * @param signal
   * @return
   */
  def checkSignal(signal: Signal): Boolean = {
    //Check if a signal is not failled
    if (signal.x == "*")
      return false
    // Check if the RTT exist
    else if (signal.rtt == None)
      return false

    else if (signal.rtt.get <= 0) {
      return false
    } else if (javatools.Tools.isPrivateIp(signal.from.get))
      return false
    else {
      return true
    }
  }

  /**
   * Calculate the median by signal IP address
   */
  def computeMedianRTTByhop(traceroute: Traceroute): MedianByHopTraceroute = {
    val hops = traceroute.result
    val procHops = hops.map(f => findMedianFromSignals(f))

    MedianByHopTraceroute(traceroute.dst_name, traceroute.from, traceroute.prb_id, traceroute.msm_id, traceroute.timestamp, procHops)
  }

  /**
   * Organize the links in a DiffRtt object
   */
  def resumeLinksTraceroute(traceroute: LinksTraceroute): Seq[DetailedLink] = {
    val links = traceroute.links
    val resumedLinks = links.map(f => DetailedLink(f.rttDiff, LinkIPs(f.ip1, f.ip2), traceroute.prb_id))
    resumedLinks
  }

  /**
   * Proceed a given Hop by grouping the signal by IP address
   *
   * @param hop Hop instance
   *
   * @return ProceedResult
   */
  def findMedianFromSignals(hop: Hop): PreparedHop = {

    val signals = hop.result
    val a = signals.groupBy(_.from.get)
    val c = a map { case (from, signaux) => from -> (getRtts(signaux)) }

    val b = c.map(f => PreparedSignal(medianCalculator(f._2), f._1))

    val d = b.toSeq
    return PreparedHop(d, hop.hop)

  }

  /**
   * Calculate the median of the given sequence of doubles
   * @param seq  sequence of doubles
   * @return the calculated median
   */
  def medianCalculator(seq: Seq[Double]): Double = {
    val sortedSeq = seq.sortWith(_ < _)

    if (seq.size % 2 == 1) sortedSeq(sortedSeq.size / 2)
    else {
      val (up, down) = sortedSeq.splitAt(seq.size / 2)
      (BigDecimal(up.last + down.head) / 2).toDouble
    }
  }
  /**
   *
   * Retrieve the RTTs by given signals
   *
   * @param signals the signals, by one hop, having the same IP address
   *
   * @return the sequence of RTT of each signal
   */
  def getRtts(signals: Seq[Signal]): Seq[Double] = {
    val t = signals.map(f => f.rtt.get)
    return t
  }

  def generateDatesSample(size: Int, date: Int): Seq[Int] = {
    //val mydate=
    // val x = java.time.LocalDateTime.ofEpochSecond(date,0,java.time.ZoneOffset.UTC)
    List.tabulate(size)(n => date).toSeq

  }

  /**
   * Sort the links by sorting the two IPs address
   */
  def sortLinks(diffRtt: DetailedLink): DetailedLink = {

    val link = Seq(diffRtt.link.ip1, diffRtt.link.ip2)
    val sortedLink = link.sorted

    diffRtt.link = LinkIPs(sortedLink(0), sortedLink(1))

    diffRtt
  }

  // TODO add documentation
  def createSamples(allRttsDiffs: Seq[Double], sizeSample: Int): Seq[Seq[Double]] = {

    var samplesRttsDiffs: Seq[Seq[Double]] = Seq()
    val total = allRttsDiffs.size
    var a = 0
    while (a * sizeSample < total) {
      val sample = allRttsDiffs.slice(a * sizeSample, a * sizeSample + sizeSample)
      val samplesTmp = samplesRttsDiffs :+ sample
      samplesRttsDiffs = samplesTmp
      a = a + 1
    }

    samplesRttsDiffs

  }

  def createHistogrammBins() {
    val rttDiffs = Array(2, 7, -1, 0.45, 10, 7)
    val num_bins = 20
    val max_rttdiff = rttDiffs.reduceLeft(_ max _)
    val min_rttdiff = rttDiffs.reduceLeft(_ min _)
    val largClasse = (max_rttdiff - min_rttdiff) / num_bins; // calcule de largeur d'une classe
    println(largClasse)
  }

  /**
   * The configuration file contains the inputs of the program that can be changed.
   * For example the folder containing the traceroutes files
   *
   * @author hayat
   * @return a configuration object
   */
  def loadConfiguration(): Config = {
    val confFilePath = "/home/hayat/rttDelaysAnalysis.conf"

    if (!checkFileExist(confFilePath)) {
      println("Configuration file (%s) not exist ".format(confFilePath))
      System.exit(0)
    }
    val confFile = Paths.get(confFilePath).toFile
    ConfigFactory.parseFile(confFile)

  }

  def checkFileExist(confFilePath: String): Boolean = {
    var isExist = true
    if (new java.io.File(confFilePath).exists) {

    } else {
      println("File (%s) not exist ".format(confFilePath))
      isExist = false
    }
    isExist
  }

  def listAlarms(spark: SparkSession, rawDataLinkFiltred: ResumedLink, timewindow: Int, rangeDates: Seq[Int]): String = {
    // Save the reference state of a link
    var reference = LinkState(Seq(), Seq(), Seq(), Seq())

    // Save the current state of a link
    var current = LinkState(Seq(), Seq(), Seq(), Seq())

    // Save the RTT differentials anomalies
    var alarmsValues = AlarmsValues()

    // Save the dates having delay anomalies
    var alarmsDates = AlarmsDates()

    // Save all the dates to draw the evolution
    var dates = AllDates()

    val rawDataLink = rawDataLinkFiltred

    /*Regardless of the period specified in the inputs, the evolution is created for one or more days
     * Eg : if the period is only 2 hours, the evolution is created for 24 hours,
     * and the begin date is the begin date given in inputs
     * */
    val start = rawDataLink.bins.min
    val max = rawDataLink.bins.max
    val diferenceDays = (max - start) / 60 / 60 / 24
    val end = start + ((diferenceDays + 1) * 86400)

    //Find all the bins in the selected days
    val datesEvolution = start.to(end - timewindow).by(timewindow)

    // For each bin, find the data (RTTs differentials) and find alarms
    datesEvolution.foreach(f => findAlarms(spark, f, reference, rawDataLink, current, alarmsDates, alarmsValues, dates))

    // create a JSON string to save the results
    implicit val formats = DefaultFormats
    val linkEvolution = LinkEvolution(rawDataLink.link, reference, current, alarmsDates.dates, alarmsValues.medians, dates.dates)
    val linkEvolutionJsonStr = write(linkEvolution)
    linkEvolutionJsonStr
  }

  def findAlarms(spark: SparkSession, date: Int, reference: LinkState, dataPeriod: ResumedLink, current: LinkState, alarmsDates: AlarmsDates, alarmsValues: AlarmsValues, dates: AllDates): Unit = {
    println("Find indices ...")
    val indices = dataPeriod.bins.zipWithIndex.filter(_._1 == date).map(_._2)
    val dist = indices.map(f => dataPeriod.rttDiffs(f))

    println("Find RTTs for the current timewindow ...")
    val distSize = dist.size

    if (distSize > 3) {
      val tmpDates = dates.dates :+ date
      dates.dates = tmpDates

      // Compute the Wilson Score
      val wilsonCi = scoreWilsonScoreCalculator(spark, dist.size).map(f => f * dist.size)

      //update the current link state
      updateLinkCurrentState(spark, dist, current, wilsonCi)

      //Sort the distribution
      val newDist = dist.sorted

        println("newDist "+newDist.toString())
      //Get the reference
      val tmpReference = reference

      // Case : 1
      if (tmpReference.valueMedian.size < 3) {

        val newReferenceValueMedian = tmpReference.valueMedian :+ current.valueMedian.last
        val newReferenceValueHi = tmpReference.valueHi :+ newDist(javatools.JavaTools.getIntegerPart(wilsonCi(1)))
        val newReferenceValueLow = tmpReference.valueLow :+ newDist(javatools.JavaTools.getIntegerPart(wilsonCi(0)))

        reference.valueHi = newReferenceValueHi
        reference.valueLow = newReferenceValueLow
        reference.valueMedian = newReferenceValueMedian
      } //Case : 2
      else if (reference.valueMedian.size == 3) {

        val newReferenceValueMedian1 = tmpReference.valueMedian :+ medianCalculator(tmpReference.valueMedian)
        val newReferenceValueHi1 = tmpReference.valueHi :+ medianCalculator(tmpReference.valueHi)
        val newReferenceValueLow1 = tmpReference.valueLow :+ medianCalculator(tmpReference.valueLow)

        reference.valueHi = newReferenceValueHi1
        reference.valueLow = newReferenceValueLow1
        reference.valueMedian = newReferenceValueMedian1

        val newReferenceValueMedian = reference.valueMedian.map(f => reference.valueMedian.last)
        reference.valueMedian = newReferenceValueMedian
        val newReferenceValueHi = reference.valueHi.map(f => reference.valueHi.last)
        reference.valueHi = newReferenceValueHi
        val newReferenceValueLow = reference.valueLow.map(f => reference.valueLow.last)
        reference.valueLow = newReferenceValueLow

      } //Case : 3
      else {

        val newReferenceValueMedian2 = tmpReference.valueMedian :+ (0.99 * tmpReference.valueMedian.last + 0.01 * current.valueMedian.last)
        val newReferenceValueHi2 = tmpReference.valueHi :+ (0.99 * tmpReference.valueHi.last + 0.01 * newDist(javatools.JavaTools.getIntegerPart(wilsonCi(1))))
        val newReferenceValueLow2 = tmpReference.valueLow :+ (0.99 * tmpReference.valueLow.last + 0.01 * newDist(javatools.JavaTools.getIntegerPart(wilsonCi(0))))
        reference.valueHi = newReferenceValueHi2
        reference.valueLow = newReferenceValueLow2
        reference.valueMedian = newReferenceValueMedian2

        //Anomalies dection : compare the current with the reference
        if ((BigDecimal(current.valueMedian.last) - BigDecimal(current.valueLow.last) > reference.valueHi.last || current.valueMedian.last + current.valueHi.last < reference.valueLow.last) && scala.math.abs(current.valueMedian.last - reference.valueMedian.last) > 1) {

          val updateAlarmsDates = alarmsDates.dates :+ date
          alarmsDates.dates = updateAlarmsDates

          val updateAlarmsValues = alarmsValues.medians :+ current.valueMedian.last
          alarmsValues.medians = updateAlarmsValues
        }

      }
      println("After reference " + reference.toString())
    }

  }

  def scoreWilsonScoreCalculator(spark: SparkSession, distSize: Int): Seq[Double] = {
    val sqlContext = spark.sqlContext
    val score = new org.apache.commons.math3.stat.interval.WilsonScoreInterval().createInterval(distSize, distSize / 2, 0.95)
    Seq(score.getLowerBound, score.getUpperBound)
  }

  def updateLinkCurrentState(spark: SparkSession, dist: Seq[Double], current: LinkState, wilsonCi: Seq[Double]) {

    val median = medianCalculator(dist)
    val newDist = dist.sorted

    //println(current.toString())
    val tmp = current
    val newValueMedian = tmp.valueMedian :+ median
    current.valueMedian = newValueMedian

    val newValueLow = tmp.valueLow :+ (BigDecimal(tmp.valueMedian.last) - BigDecimal(newDist(javatools.JavaTools.getIntegerPart(wilsonCi(0))))).toDouble
    val newValueHi = tmp.valueHi :+ (BigDecimal(newDist(javatools.JavaTools.getIntegerPart(wilsonCi(1)))) - BigDecimal(current.valueMedian.last)).toDouble

    current.valueHi = newValueHi
    current.valueLow = newValueLow

    println("newDist(javatools.JavaTools.getIntegerPart(wilsonCi(0)))) est " + newDist(javatools.JavaTools.getIntegerPart(wilsonCi(0))))
    println("newDist(javatools.JavaTools.getIntegerPart(wilsonCi(1)))) est " + newDist(javatools.JavaTools.getIntegerPart(wilsonCi(1))))

  }

  def signalToString(s: Signal): String = {
    return "(from : %s, rtt : %s, x : %s)".format(s.from.get, s.rtt.get.toString(), s.x)
  }
  def hopToString(h: Hop): String = {
    return "hop_id : %d, hops : [%s]".format(h.hop, h.result.map(signalToString).mkString(","))

  }
  def tracerouteToString(t: Traceroute): String = {
    return "(dst_name : %s, from : %s, msm_id : %d, prb_id : %d, hops: (%s))".format(t.dst_name, t.from, t.msm_id, t.prb_id, t.result.map(hopToString).mkString(","))
  }
  def traceroutesPerPeriodToString(tp: TraceroutesPerPeriod): String = {
    return "(start_bin : %d, traceroutes : [%s])".format(tp.timeWindow, tp.traceroutes.map(i => tracerouteToString(i)).mkString(","))
  }
}