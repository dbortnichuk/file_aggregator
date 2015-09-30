package ua.softserve.spark

/**
 * Created by dbort on 30.09.2015.
 */
package object aggregator {

  def debugCountdown(seconds: Int) = {
    println("-------------Attach debugger now, " + seconds + " seconds left!--------------") //$ export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
    Thread.sleep(seconds * 1000)
    println("-------------Debugger should be connected by now for successful debugging!--------------")
  }

}
