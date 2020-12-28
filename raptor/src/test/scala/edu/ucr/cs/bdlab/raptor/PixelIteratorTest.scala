package edu.ucr.cs.bdlab.raptor

import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PixelIteratorTest extends FunSuite with ScalaSparkTest {

  test("EmptyResults") {
    val rasterFile = makeFileCopy("/rasters/MYD11A1.A2002185.h09v06.006.2015146150958.hdf").getPath
    val i = Array(
      (0.toLong, (0.toLong, 0, 2, 5)),
      (0.toLong, (0.toLong, 1, 5, 9))
    )
    val pixelIterator = new PixelIterator(i.iterator, Array(rasterFile), "LST_Day_1km")
    assert(pixelIterator.isEmpty)
  }

  test("NonEmptyResults") {
    val rasterFile = makeFileCopy("/rasters/MYD11A1.A2002185.h09v06.006.2015146150958.hdf").getPath
    val i = Array(
      (0.toLong, (0.toLong, 0, 0, 7)),
      (0.toLong, (0.toLong, 2, 0, 3))
    )
    val pixelIterator = new PixelIterator(i.iterator, Array(rasterFile), "LST_Day_1km")
    val values: Array[(Long, Int, Int, Int, Float)] = pixelIterator.toArray
    assert(values.length == 4)
    assert(values(0)._5 == (15240 / 50.0).toFloat)
    assert(values(1)._5 == (15240 / 50.0).toFloat)
    assert(values(2)._5 == (15349 / 50.0).toFloat)
    assert(values(3)._5 == (15383 / 50.0).toFloat)
  }
}
