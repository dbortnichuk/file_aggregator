package ua.softserve.spark.aggregator

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import AggDriver._

/**
 * Created by dbort on 08.10.2015.
 */
class AggSuite extends FunSuite with BeforeAndAfterAll {

  lazy val sc: SparkContext = new SparkContext("local", getClass.getSimpleName)

  def path(file: String) = getClass.getResource("/" + file).getFile

  test("combineTextFileSuccessfully") {
    val output = sc.combineTextFiles(path("testinput"), 128, 128, "\n", "true").collect.sorted
    assert(output.deep == Array("1", "2", "3", "4").deep)
  }

  override def afterAll() {
    sc.stop()
  }

}
