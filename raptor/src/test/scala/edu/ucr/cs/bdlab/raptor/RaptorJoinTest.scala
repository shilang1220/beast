package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{CoordinateSequence, Envelope, GeometryFactory}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RaptorJoinTest extends FunSuite with ScalaSparkTest {

  val factory = new GeometryFactory()
  def createSequence(points: (Double, Double)*): CoordinateSequence = {
    val cs = factory.getCoordinateSequenceFactory.create(points.length, 2)
    for (i <- points.indices) {
      cs.setOrdinate(i, 0, points(i)._1)
      cs.setOrdinate(i, 1, points(i)._2)
    }
    cs
  }

  test("RaptorJoinZS") {
    val rasterFile = makeFileCopy("/raptor/glc2000_small.tif").getPath
    val testPoly = factory.toGeometry(new Envelope(-82.76, -80.25, 31.91, 35.17))
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, new Feature(testPoly))))
    val rasters: Array[String] = Array(rasterFile)

    val values: RDD[(Long, Int, Int, Int, Float)] = RaptorJoin.raptorJoin(rasters, vector, new BeastOptions())

    // Values sorted by (x, y)
    val finalValues: Array[(Long, Int, Int, Int, Float)] = values.collect().sortWith((a, b) => a._3 < b._3 || a._3 == b._3 && a._4 < b._4)
    assert(finalValues.length == 6)
    assert(finalValues(0)._3 == 69)
    assert(finalValues(0)._4 == 48)
  }

  test("EmptyResult") {
    val rasterFile = makeFileCopy("/raptor/glc2000_small.tif").getPath
    val testPoly = factory.createPolygon()
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, new Feature(testPoly))))
    val rasters: Array[String] = Array(rasterFile)

    val values: RDD[(Long, Int, Int, Int, Float)] = RaptorJoin.raptorJoin(rasters, vector, new BeastOptions())

    // Values sorted by (x, y)
    val finalValues: Array[(Long, Int, Int, Int, Float)] = values.collect().sortWith((a, b) => a._3 < b._3 || a._3 == b._3 && a._4 < b._4)
    assert(finalValues.isEmpty)
  }
}
