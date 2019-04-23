package ripeatlasanalysis

import java.util.Date

object classes {

  /**
   * AlarmsDates is used to model the list of bins having anomalies.
   */
  case class AlarmsDates(
    var dates: Seq[Int] = Seq())

  /**
   * AlarmsValues is used to model the values of the differential RTTs representing an anomaly.
   */
  case class AlarmsValues(
    var medians: Seq[Double] = Seq())

  /**
   * AllDates is used to save
   */
  case class AllDates(
    var dates: Seq[Int] = Seq())

  /**
   * Hop model a hop of a traceroute
   *
   * hop is the id of the hop
   *
   * result is the list of signals of the hop
   */
  case class Hop(
    var result: Seq[Signal],
    hop:        Int)

  /**
   * Traceroute is used to model a traceroute
   *
   * dst_name : the destination IP address of the traceroute
   * from:  the IP address of probe
   * prb_id:   the ID of the probe
   * msm_id:   the ID of the measurement
   * timestamp: the time of the timestamp
   * result:    the list of the hops
   *
   */
  case class Traceroute(
    dst_name:  String,
    from:      String,
    prb_id:    BigInt,
    msm_id:    BigInt,
    timestamp: BigInt,
    result:    Seq[Hop])

  /**
   * PreparedSignal is a prepared Signal created by aggregating the signals from the same router in a hop
   * medianRtt is the median of all rtts receved from
   */
  case class PreparedSignal(
    medianRtt: Double,
    from:      String)

  /**
   * PreparedHop is the hop that contains the prepared signals.
   * when a hop receive the three signals from same router, the result is only one signal
   * when a hop receive the signals from more than router, the PreparedHop  contains more than one prepared signal.
   */
  case class PreparedHop(
    var result: Seq[PreparedSignal],
    hop:        Int)

  /**
   * MedianByHopTraceroute is a traceroute having prepared hops (PreparedHop)
   */
  case class MedianByHopTraceroute(
    dst_name:  String,
    from:      String,
    prb_id:    BigInt,
    msm_id:    BigInt,
    timestamp: BigInt           = 0,
    result:    Seq[PreparedHop])

  /**
   * Link model the the two IP address of a link and its differential RTT
   */
  case class Link(
    ip1:     String,
    ip2:     String,
    rttDiff: Double)

  /**
   * LinksTraceroute model a traceroute where the hops are replaced by links
   */
  case class LinksTraceroute(
    dst_name:  String,
    from:      String,
    prb_id:    BigInt,
    msm_id:    BigInt,
    timestamp: BigInt,
    links:     Seq[Link])

  /**
   * LinkIPs model the two IP address of a link
   */
  case class LinkIPs(
    ip1: String,
    ip2: String)

  /**
   * DetailedLink model a link with more details than its differential RTT, eg. probe identifying the link
   */
  case class DetailedLink(
    rttDiff:  Double,
    var link: LinkIPs,
    probe:    BigInt)

  /**
   * ResumedLink model a link after the  data preparation stage. Each link is described by its RTT differential, its probe and the bin when it is identified
   */
  case class ResumedLink(
    link:     LinkIPs,
    probes:   Seq[BigInt],
    rttDiffs: Seq[Double],
    var bins: Seq[Int])

  /**
   * TraceroutesPerPeriod model a set of traceroutes and the period when these traceroutes are captured.
   */
  case class TraceroutesPerPeriod(
    traceroutes: Seq[Traceroute],
    timeWindow:  Int)

  /**
   * TracerouteWithTimewindow model each traceroute with the period it belong to
   */
  case class TracerouteWithTimewindow(
    traceroute: Traceroute,
    period:     Int)

  /**
   * LinkState is used to model the state of a link at a given period : the median RTT differential, the average RTT differential and the confident interval
   */
  case class LinkState(
    var valueMedian: Seq[Double],
    var valueHi:     Seq[Double],
    var valueLow:    Seq[Double],
    var valueMean:   Seq[Double])

  /**
   * LinkEvolution is used to model the result of a link analysis,  specially this class is used to save the result of the analysis on json file.
   */
  case class LinkEvolution(
    link:         LinkIPs,
    reference:    LinkState,
    current:      LinkState,
    alarmsDates:  Seq[Int],
    alarmsValues: Seq[Double],
    dates:        Seq[Int])

  /**
   * Signal is used to model a signal in a hop. Each signal is defined by the router transmitted the signal, the RTT
   */
  case class Signal(
    rtt:  Option[Double],
    x:    Option[String],
    from: Option[String])
}