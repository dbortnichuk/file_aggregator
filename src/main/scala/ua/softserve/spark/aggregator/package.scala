package ua.softserve.spark

import java.net.URI
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Created by dbort on 30.09.2015.
 */
package object aggregator {

  val FileNameConfDefault = "/conf.properties"

  val PropAppName = "appName"
  val PropMasterURI = "masterURI"
  val PropInputDir = "inputDir"
  val PropOutputDir = "outputDir"
  val PropMaxFileSize = "maxFileSize"
  val PropHdfsBlockSize = "hdfsBlockSize"
  val PropOutputFileContentDelim = "outputFileContentDelim"
  val PropInputDirRecursiveRead = "inputDirRecursiveRead"

  def debugCountdown(seconds: Int) = { //turn on debug - $ export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
    println("-------------Attach debugger now, " + seconds + " seconds left!--------------")
    Thread.sleep(seconds * 1000)
    println("-------------Debugger should be connected by now for successful debugging!--------------")
  }

  def dumpConfig(conf: Configuration, filePath: String): Unit ={
    val fs = FileSystem.get(URI.create(filePath), conf)
    val out = fs.create(new Path(filePath), true)
    conf.writeXml(out)
    out.close()
  }



}
