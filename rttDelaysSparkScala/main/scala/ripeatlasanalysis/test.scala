package ripeatlasanalysis

import Tools._
object test {

  def main(args: Array[String]): Unit = {
    val o = medianCalculator(Array(2.463, 3.463, 2.991, 4.631, -7.75))
    val a = BigDecimal(3.143)
    val N = BigDecimal(4.41)

    // println(new BigDecimal("3.143") - new BigDecimal("4.41"))
    println(N - a)

    val link = Seq("185.147.12.31", "89.105.200.57")
    val sortedLink = link.sorted
    println(sortedLink)
    val linkj = Seq("89.105.200.57", "185.147.12.31")
    val sortedLinkj = linkj.sorted
    println(sortedLinkj)
    
    println(o)
  }
}