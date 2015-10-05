package ua.softserve.spark.aggregator

import java.io.{FileOutputStream, OutputStream}
import java.net.URI

import org.apache.hadoop.mapred.JobConf
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
import scala.reflect.ClassTag
import scala.reflect._
import org.apache.spark.rdd._

/**
 * Created by dbort on 01.10.2015.
 */
object AggDriver {

  private val defaultSize = 256
  private val defaultDelim = "\n"

  def main(args: Array[String]) {

    //debugCountdown(8)

    val sparkContext = new SparkContext("spark://quickstart.cloudera:7077", "File_Aggregator")
    val rdd = sparkContext.aggregateTextFiles(args(0))
    rdd.saveAsTextFile(args(0) + "-out")

  }

  implicit class Aggregator(val origin: SparkContext) {

    def aggregateTextFiles(inDirPath: String, size: Long = defaultSize, delim: String = defaultDelim): RDD[String] = {

      val hadoopConf = origin.hadoopConfiguration
      hadoopConf.set("textinputformat.record.delimiter", delim)
      hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
      hadoopConf.set("mapred.input.dir", inDirPath)
      hadoopConf.setLong("mapred.max.split.size", size * 1024 * 1024)
      hadoopConf.setBoolean("fs.hdfs.impl.disable.cache", true)
      hadoopConf.setLong("dfs.blocksize", 128 * 1024 * 1024) //check by - example: hadoop fs -stat %o /user/examples1/files-out/part-00002

      val jobConf = new JobConf(hadoopConf)

      //jobConf.setOutputFormat()

      dumpConfig(jobConf, "hdfs://localhost/user/examples1/props/props.txt")

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
