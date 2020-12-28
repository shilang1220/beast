package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.geolite.GeometryReader
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IntersectionIteratorTest extends FunSuite with ScalaSparkTest {

  test("Iterate over two intersections") {
    val raster = new FakeRaster(1, 1, 10, 10)
    val geometries1: Array[Geometry] = Array(
      GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(1, 1)),
      GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(3, 3)),
    )
    val intersections1 = new Intersections
    intersections1.compute(geometries1, raster)

    val geometries2: Array[Geometry] = Array(
      GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(2, 2)),
      GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(4, 4)),
    )
    val intersections2 = new Intersections
    intersections2.compute(geometries2, raster)

    val rasterIDs: Array[Int] = Array(0, 1)
    val intersectionsList: Array[Intersections] = Array(intersections1, intersections2)
    val intersectionsIterator = new IntersectionsIterator(rasterIDs, intersectionsList)
    assert(intersectionsIterator.size == 4)
  }
}
