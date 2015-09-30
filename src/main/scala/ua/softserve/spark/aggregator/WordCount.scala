package ua.softserve.spark.aggregator

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dbort on 30.09.2015.
 */
object WordCount {

  def main (args: Array[String]) {

    println("WordCount: Hello, User. I've started execution.")

    val sparkConf = new SparkConf()
    sparkConf.setAppName("WordCount")

    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.textFile(args(0))
    val wc = rdd.flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wc.saveAsTextFile(args(1))


    println("WordCount: Hello, User. I've ended execution.")

  }

}
