package ua.softserve.spark.aggregator

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dbort on 29.09.2015.
 */
object Aggregator {

  def main (args: Array[String]) {

    println("Aggregator: Hello, User. I've started execution.")

    val sparkConf = new SparkConf()
    sparkConf.setAppName("Aggregator")

    val sparkContext = new SparkContext(sparkConf)



    println("Aggregator: Hello, User. I've ended execution.")

  }

}
