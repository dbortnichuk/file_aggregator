package ua.softserve.spark.aggregator

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dbort on 29.09.2015.
 */
object Aggregator_legacy {

  def main (args: Array[String]) {

    println("Aggregator: I've started execution.")
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Aggregator")

    debugCountdown(8)

    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.wholeTextFiles(args(0))


    //rdd.saveAsSequenceFile(args(0) + "-seq")

    rdd.map(f => new Tuple2(new Text(f._1), new Text(f._2))).saveAsNewAPIHadoopFile(args(0) + "-seq", classOf[Text], classOf[Text], classOf[SequenceFileOutputFormat[Text, Text]])

    val rddSeq = sparkContext.sequenceFile(args(0) + "-seq", classOf[Text], classOf[Text])

    println("Aggregator: I've ended execution.")

  }

}
