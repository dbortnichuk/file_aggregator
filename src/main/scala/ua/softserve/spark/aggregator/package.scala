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
 * Utility object to hold constants and auxiliary functions.
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

  val MsgUsage = "spark-submit --class \"ua.softserve.spark.aggregator.AggDriver\" *.jar"
  val MsgIn = "\"in\" is a required property, specify it to point out directory URI small files to be read and aggregated into combined files, example: hdfs://localhost/user/examples/files"
  val MsgOut = "\"out\" is a required property, specify it to point out directory URI to upload combined files to, example: hdfs://localhost/user/examples/files-out"
  val MsgMaster = "\"master\" is a required property, specify it to point out spark master URI, example: spark://quickstart.cloudera:7077"
  val MsgName = "\"name\" is an optional property, Application display name, default: File_Aggregator"
  val MsgMaxFileSize = "\"fsize\" is an optional property, Max size of single output file in Mb, default: 128"
  val MsgHdfsBlockSize = "\"bsize\" is an optional property, HDFS file block size in Mb, default: governed by dfs.blocksize Hadoop option"
  val MsgOutputFileContentDelim = "\"delim\" is an optional property, delimiter to separate content from small input files in combined files, default: \"linebreak\""
  val MsgInputDirRecursiveRead = "\"recursive\" is an optional property, enables recursive file reads in nested directories, default: true"
  val MsgHelp = "Use this option to check application usage"
  val MsgNote = "NOTE: arguments with spaces should be enclosed in \" \"."

  val Mb = 1024 * 1024

  def debugCountdown(seconds: Int) = {
    //turn on debug - $ export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
    println("-------------Attach debugger now, " + seconds + " seconds left!--------------")
    Thread.sleep(seconds * 1000)
    println("-------------Debugger should be connected by now for successful debugging!--------------")
  }

  def dumpConfig(conf: Configuration, filePath: String): Unit = {
    val fs = FileSystem.get(URI.create(filePath), conf)
    val out = fs.create(new Path(filePath), true)
    conf.writeXml(out)
    out.close()
  }


}
