package ua.softserve.spark.aggregator

import org.apache.hadoop.io._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dbort on 29.09.2015.
 */
object Aggregator {

  def main (args: Array[String]) {

    println("Aggregator: I've started execution.")
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Aggregator")

    //debugCountdown(8)

    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.wholeTextFiles(args(0))


    //rdd.saveAsSequenceFile(args(0) + "-seq")

    val rddSeq = sparkContext.sequenceFile(args(0) + "-seq", classOf[Text], classOf[Text])



    println("Aggregator: I've ended execution.")

  }

}
