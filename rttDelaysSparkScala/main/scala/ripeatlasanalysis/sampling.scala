package ripeatlasanalysis

//imports
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import classes._
import ripeatlasanalysis.Tools.generateDateSample
import Tools._
object sampling {

  def main(args: Array[String]): Unit = {
    // Spark configuration :  specify master type
    val conf = new SparkConf().setAppName("RTT delays analysis").setMaster("local")

    //Spark configuration :  create Spark session
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val rttDiffStrings = spark.sparkContext.textFile("/home/162558/spark/RipeAtlasTraceroutesAnalysis/rttDiffDistrinution/data_set_plus_link_1000.csv")
    val rttDiffDoubleRDD = rttDiffStrings.map(f => f.toDouble)
    val rttDiffDoubleSeq = rttDiffDoubleRDD.collect()
    println(rttDiffDoubleSeq.size)

    val samplesRTTsDiffs= createSamples(rttDiffDoubleSeq, 20)
    val medianSamplesRTTsDiffs = samplesRTTsDiffs.map(medianCalculator)
    
    println(medianSamplesRTTsDiffs.mkString("\n"))
    
    
    
    
  }
}