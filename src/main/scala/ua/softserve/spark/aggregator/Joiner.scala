package ua.softserve.spark.aggregator

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dbort on 30.09.2015.
 */
object Joiner {

  def main (args: Array[String]) {

    println("Joiner: Hello, User. I've started execution.")

    val sparkConf = new SparkConf()
    sparkConf.setAppName("Joiner")
    //sparkConf.set("spark.ui.port", "36000")
    //sparkConf.set("spark.master", "local[4]")

    val sparkContext = new SparkContext(sparkConf)

    debugCountdown(8)

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val rddClk = sparkContext.textFile(args(0)).map(_.split("\t")).map(c => (c(1), Click(format.parse(c(0)), c(1), c(2).trim.toInt)))
    val rddReg = sparkContext.textFile(args(1)).map(_.split("\t")).map(r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat)))

    println("JOIN: " + rddReg.join(rddClk).collect())

    println("Joiner: Hello, User. I've ended execution.")

  }

  private case class Click(d: java.util.Date, uuid: String, landing_page: Int)

  private case class Register(d: java.util.Date, uuid: String, cust_id: String, lat: Float, lng: Float)

  def debugCountdown(seconds: Int) = {
    println("-------------Attach debugger now, " + seconds + " seconds left!--------------") //$ export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
      Thread.sleep(seconds * 1000)
    println("-------------Debugger should be connected by now for successful debugging!--------------")
  }

}
