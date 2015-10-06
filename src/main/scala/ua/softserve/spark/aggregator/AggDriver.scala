package ua.softserve.spark.aggregator

import java.io.{File, FileOutputStream, OutputStream}
import java.net.{URL, URI}
import java.util.Properties

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.util.LineReader
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect._
import org.apache.spark.rdd._


/**
 * Created by dbort on 01.10.2015.
 */
object AggDriver {

  def main(args: Array[String]) {

//    val source = if (args.size > 0) Source.fromURL(args(0)) else Source.fromURL(getClass.getResource(FileNameConfDefault))
//    val properties = new Properties()
//    properties.load(source.bufferedReader())
//
//
      //debugCountdown(6)
//
//    val sparkContext = new SparkContext("spark://quickstart.cloudera:7077", "File_Aggregator")
//    val rdd = sparkContext.combineTextFiles(properties.getProperty(PropInputDir))
//    rdd.saveAsTextFile(properties.getProperty(PropOutputDir))

    val parser = new scopt.OptionParser[Config](MsgUsage) {
      head("File Aggregator", "1.0")
      opt[String]('i', "in") required() valueName("<inURI>") action { (x, c) =>
        c.copy(in = x) } text(MsgIn)
      opt[String]('o', "out") required() valueName("<outURI>") action { (x, c) =>
        c.copy(out = x) } text(MsgOut)
      opt[String]('m', "master") required() valueName("<masterURI>") action { (x, c) =>
        c.copy(master = x) } text(MsgMaster)
      opt[String]('n', "name") valueName("<appName>") action { (x, c) =>
        c.copy(name = x) } text(MsgName)
      opt[Long]('f', "fsize") valueName("<fileSize>") action { (x, c) =>
        c.copy(maxFileSize = x) } text(MsgMaxFileSize)
      opt[Long]('b', "bsize") valueName("<blockSize>") action { (x, c) =>
        c.copy(hdfsBlockSize = x) } text(MsgHdfsBlockSize)
      opt[String]('d', "delim") valueName("<delimiter>") action { (x, c) =>
        c.copy(outputFileContentDelim = x) } text(MsgOutputFileContentDelim)
      opt[Boolean]('r', "recursive") valueName("<recursiveRead>") action { (x, c) =>
        c.copy(inputDirRecursiveRead = x) } text(MsgInputDirRecursiveRead)
      help("help") text(MsgHelp)
      note(MsgNote)
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        val sparkContext = new SparkContext(config.master, config.name)
        val rdd = sparkContext.combineTextFiles(config.in,
          config.maxFileSize, config.hdfsBlockSize, config.outputFileContentDelim, config.inputDirRecursiveRead.toString)
        rdd.saveAsTextFile(config.out)
      case None => println("ERROR: bad argument set provided")
    }


//    rdd
//      .map{v => (new Text(args(0) + "-out"), new Text(v.toString))}
//      .saveAsHadoopDataset(new JobConf(sparkContext.hadoopConfiguration))

  }

  case class Config(in: String = "",
                    out: String = "",
                    master: String = "",
                    name: String = "File Aggregator",
                    maxFileSize: Long = 128,
                    hdfsBlockSize: Long = -1,
                    outputFileContentDelim: String = "\n",
                    inputDirRecursiveRead: Boolean = true)

  implicit class Aggregator(val origin: SparkContext) {

    def combineTextFiles(inDirPath: String, fSize: Long, bSize: Long, delim: String, recursiveRead: String): RDD[String] = {

      val hadoopConf = origin.hadoopConfiguration
      hadoopConf.set("textinputformat.record.delimiter", delim)
      hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", recursiveRead)
      hadoopConf.set("mapred.input.dir", inDirPath)
      hadoopConf.setLong("mapred.max.split.size", fSize * Mb)
      hadoopConf.setBoolean("fs.hdfs.impl.disable.cache", true)
      if (bSize > 0) hadoopConf.setLong("dfs.blocksize", bSize * Mb) //check by - example: hadoop fs -stat %o /user/examples1/files-out/part-00002
      val jobConf = new JobConf(hadoopConf)
      //jobConf.setOutputFormat(classOf[KeyBasedMultipleTextOutputFormat])
      //dumpConfig(jobConf, "hdfs://localhost/user/examples1/props/props.txt")
      origin.newAPIHadoopRDD(jobConf, classOf[CombineTextFileWithOffsetInputFormat], classOf[LongWritable], classOf[Text]).map(_._2.toString)
      //origin.newAPIHadoopFile(inDirPath, classOf[CombineTextFileWithOffsetInputFormat], classOf[LongWritable], classOf[Text], hadoopConf).map(_._2.toString)

    }
  }

  private class CombineTextFileWithOffsetInputFormat extends CombineFileInputFormat[LongWritable, Text] {
    override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] =
      new CombineFileRecordReader(split.asInstanceOf[CombineFileSplit], context, classOf[CombineTextFileWithOffsetRecordReader])
  }

  private class CombineTextFileWithOffsetRecordReader(split: CombineFileSplit, context: TaskAttemptContext, index: Integer)
    extends CombineTextFileRecordReader[LongWritable](split, context, index) {

    override def generateKey(split: CombineFileSplit, index: Integer) = split.getOffset(index)
  }

//  class KeyBasedMultipleTextOutputFormat extends MultipleTextOutputFormat[Text, Text] {
//    override def generateFileNameForKeyValue(key: Text, value: Text, name: String): String = {
//      key.toString + "/" + name
//    }
//
//    override def generateActualKey(key: Text, value: Text) = null
//  }


  private abstract class CombineTextFileRecordReader[K](split: CombineFileSplit, context: TaskAttemptContext, index: Integer)
    extends RecordReader[K, Text] {

    val conf = context.getConfiguration
    val path = split.getPath(index)
    val fs = path.getFileSystem(conf)
    val codec = Option(new CompressionCodecFactory(conf).getCodec(path))

    val start = split.getOffset(index)
    val length = if (codec.isEmpty) split.getLength(index) else Long.MaxValue
    val end = start + length

    val fd = fs.open(path)
    if (start > 0) fd.seek(start)

    val fileIn = codec match {
      case Some(codec) => codec.createInputStream(fd)
      case None => fd
    }

    var reader = new LineReader(fileIn)
    var pos = start

    def generateKey(split: CombineFileSplit, index: Integer): K

    protected val key = generateKey(split, index)
    protected val value = new Text

    override def initialize(split: InputSplit, ctx: TaskAttemptContext) {}

    override def nextKeyValue(): Boolean = {
      if (pos < end) {
        val newSize = reader.readLine(value)
        pos += newSize
        newSize != 0
      } else {
        false
      }
    }

    override def close(): Unit = if (reader != null) {
      reader.close();
      reader = null
    }

    override def getCurrentKey: K = key

    override def getCurrentValue: Text = value

    override def getProgress: Float = if (start == end) 0.0f else math.min(1.0f, (pos - start).toFloat / (end - start))
  }


}
