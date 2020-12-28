/*
 * Copyright 2020 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.beast.io
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, PointND}
import edu.ucr.cs.bdlab.beast.cg.Reprojector.TransformationInfo
import org.apache.spark.test.ScalaSparkTest
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.{DefaultGeographicCRS, DefaultProjectedCRS}
import org.geotools.referencing.cs.{DefaultCartesianCS, DefaultEllipsoidalCS}
import org.geotools.referencing.datum.{DefaultEllipsoid, DefaultGeodeticDatum, DefaultPrimeMeridian}
import org.geotools.referencing.factory.OrderedAxisAuthorityFactory
import org.geotools.referencing.operation.DefaultMathTransformFactory
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, PrecisionModel}
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.cs.AxisDirection
import org.opengis.referencing.operation.MathTransform
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReprojectorTest extends FunSuite with ScalaSparkTest {
  test("findMathTransform for non EPSG CRS") {
    System.setProperty("org.geotools.referencing.forceXY", "false")
    val targetCRS: CoordinateReferenceSystem = new DefaultProjectedCRS("Sinusoidal", new DefaultGeographicCRS(new DefaultGeodeticDatum("World", DefaultEllipsoid.WGS84, DefaultPrimeMeridian.GREENWICH), DefaultEllipsoidalCS.GEODETIC_2D), //sinus.getConversionFromBase.getMathTransform,
      new DefaultMathTransformFactory().createFromWKT("PARAM_MT[\"Sinusoidal\", \n  PARAMETER[\"semi_major\", 6371007.181], \n  PARAMETER[\"semi_minor\", 6371007.181], \n  PARAMETER[\"central_meridian\", 0.0], \n  PARAMETER[\"false_easting\", 0.0], \n  PARAMETER[\"false_northing\", 0.0]]"), DefaultCartesianCS.PROJECTED);
    val transform: TransformationInfo = Reprojector.findTransformationInfo(4326, targetCRS, sparkContext.getConf)
    assert(transform != null)
  }

  test("should not reuse objects") {
    Reprojector.CachedTransformationInfo.clear()
    val point: Geometry = new GeometryFactory().createPoint(new Coordinate(1, 1))
    point.setSRID(4326)
    val transform: Geometry = Reprojector.reprojectGeometry(point, CRS.decode("EPSG:3857", true), sparkContext.getConf)
    assert(point != transform)
    assert(!point.equals(transform))

    val linestring: Geometry = new GeometryFactory().createLineString(Array(new Coordinate(1, 1), new Coordinate(2, 2)))
    linestring.setSRID(4326)
    val transformed: Geometry = Reprojector.reprojectGeometry(linestring, CRS.decode("EPSG:3857", true), sparkContext.getConf)
    assert(transformed != linestring)
    assert(!transformed.equals(linestring))
  }

  test("reproject with reversed source axes") {
    Reprojector.CachedTransformationInfo.clear()
    val dir = new Array[AxisDirection](2)
    dir(0) = AxisDirection.NORTH
    dir(1) = AxisDirection.EAST
    val factory = new OrderedAxisAuthorityFactory("EPSG", new Hints(), dir)
    val sourceCRS: CoordinateReferenceSystem = factory.createCoordinateReferenceSystem("EPSG:4326")
    val targetCRS: CoordinateReferenceSystem = CRS.decode("EPSG:3857", true)

    // Test point
    val point = new PointND(new GeometryFactory, 2, -100, 20)
    assert(Math.abs(-11131949.079327356 - Reprojector.reprojectGeometry(point, sourceCRS, targetCRS, sparkContext.getConf).getCoordinate.getX) < 1E-3)

    // Test envelope
    val envelope = new EnvelopeND(new GeometryFactory, 2, -100, 20, -100, 20)
    assert(Math.abs(-11131949.079327356 - Reprojector.reprojectGeometry(envelope, sourceCRS, targetCRS, sparkContext.getConf).getCoordinate.getX) < 1E-3)

    val geometryFactory = new GeometryFactory()
    // Test JTS Point
    val jtsPoint = geometryFactory.createPoint(new Coordinate(-100, 20))
    assert(Math.abs(-11131949.079327356 - Reprojector.reprojectGeometry(jtsPoint, sourceCRS, targetCRS, sparkContext.getConf).getCoordinate.getX) < 1E-3)

    // Test JTS LineString
    val jtsLineString = geometryFactory.createLineString(Array(new Coordinate(-100, 20), new Coordinate(-100, 20)))
    assert(Math.abs(-11131949.079327356 - Reprojector.reprojectGeometry(jtsLineString, sourceCRS, targetCRS, sparkContext.getConf).getCoordinate.getX) < 1E-3)

    val envelope2 = new Envelope(-100, -100, 20, 20)
    assert(math.abs(-11131949.079327356 - Reprojector.reprojectEnvelope(envelope2, sourceCRS, targetCRS, sparkContext.getConf).getMinX) < 1E-3)
  }


  test("reproject with reversed source and target axes") {
    Reprojector.CachedTransformationInfo.clear()
    val dir = new Array[AxisDirection](2)
    dir(0) = AxisDirection.NORTH
    dir(1) = AxisDirection.EAST
    val factory = new OrderedAxisAuthorityFactory("EPSG", new Hints(), dir)
    val sourceCRS: CoordinateReferenceSystem = factory.createCoordinateReferenceSystem("EPSG:4326")
    val targetCRS: CoordinateReferenceSystem = factory.createCoordinateReferenceSystem("EPSG:3857")

    // Test point
    val point = new PointND(new GeometryFactory, 2, -100, 20)
    assert(Math.abs(-11131949.079327356 - Reprojector.reprojectGeometry(point, sourceCRS, targetCRS, sparkContext.getConf).getCoordinate.getX) < 1E-3)

    // Test envelope
    val envelope = new EnvelopeND(new GeometryFactory, 2, -100, 20, -100, 20)
    assert(Math.abs(-11131949.079327356 - Reprojector.reprojectGeometry(envelope, sourceCRS, targetCRS, sparkContext.getConf).getCoordinate.getX) < 1E-3)

    val geometryFactory = new GeometryFactory()
    // Test JTS Point
    val jtsPoint = geometryFactory.createPoint(new Coordinate(-100, 20))
    assert(Math.abs(-11131949.079327356 - Reprojector.reprojectGeometry(jtsPoint, sourceCRS, targetCRS, sparkContext.getConf).getCoordinate.getX) < 1E-3)

    // Test JTS LineString
    val jtsLineString = geometryFactory.createLineString(Array(new Coordinate(-100, 20), new Coordinate(-100, 20)))
    assert(Math.abs(-11131949.079327356 - Reprojector.reprojectGeometry(jtsLineString, sourceCRS, targetCRS, sparkContext.getConf).getCoordinate.getX) < 1E-3)
  }

  test("Reproject multi points") {
    Reprojector.CachedTransformationInfo.clear()
    val geometryFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326)
    val multiPoint: Geometry = geometryFactory.createMultiPoint(Array(
      geometryFactory.createPoint(new Coordinate(1, 1)),
      geometryFactory.createPoint(new Coordinate(2, 2)),
    ))
    val projectedMultiPoint: Geometry = Reprojector.reprojectGeometry(multiPoint, CRS.decode("EPSG:3857", true), sparkContext.getConf)
    assert(projectedMultiPoint.getGeometryType == "MultiPoint")
    assert(!projectedMultiPoint.equals(multiPoint))
  }
}
