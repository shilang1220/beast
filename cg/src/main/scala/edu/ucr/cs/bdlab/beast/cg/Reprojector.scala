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
package edu.ucr.cs.bdlab.beast.cg

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, GeometryReader}
import org.apache.spark.SparkConf
import org.apache.spark.beast.CRSServer
import org.apache.spark.internal.Logging
import org.geotools.referencing.CRS
import org.geotools.referencing.CRS.AxisOrder
import org.locationtech.jts.geom._
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.{MathTransform, TransformException}

object Reprojector extends Logging {

  /**
   * A class that stores information for transforming geometries
   * @param sourceCRS the source CRS object. Used to determine the axis order.
   * @param targetCRS the target CRS object. Used to determine the axis order.
   * @param sourceSRID the source SRID. Used to double check that it matches the geometry.
   * @param targetSRID the target SRID. Can be used to set the target SRID.
   * @param targetFactory the geometry factory for target geometries. Used to ensure that all derivative geometries
   *                      will have the targetSRID
   */
  case class TransformationInfo(sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem,
                                sourceSRID: Int, targetSRID: Int,
                                mathTransform: MathTransform, targetFactory: GeometryFactory)


  lazy val CachedTransformationInfo: scala.collection.mutable.HashMap[String, TransformationInfo] =
    scala.collection.mutable.HashMap.empty[String, TransformationInfo]

  /**
   * Creates or retrieves a cached math transform to transform between the given two CRS
   * @param sourceCRS source coordinate reference system
   * @param targetCRS target coordinate reference system
   * @param sparkConf used to retrieve [[CRSServer]] address and port to convert the CRS to SRID for caching
   * @return the math transformation that transforms from source to destination
   */
  def findTransformationInfo(sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem, sparkConf: SparkConf): TransformationInfo = {
    val sourceSRID = CRSServer.crsToSRID(sourceCRS, sparkConf)
    val targetSRID = CRSServer.crsToSRID(targetCRS, sparkConf)
    if (sourceSRID == 0 || targetSRID == 0) {
      // Do not use cache for invalid SRID
      TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID,
        CRS.findMathTransform(sourceCRS, targetCRS, true), GeometryReader.getGeometryFactory(targetSRID))
    } else {
      val key = sourceSRID + "/" + targetSRID
      CachedTransformationInfo.getOrElseUpdate(key, {
        TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID,
          CRS.findMathTransform(sourceCRS, targetCRS, true), GeometryReader.getGeometryFactory(targetSRID))
      })
    }
  }

  /**
   * Creates or retrieves a cached math transform to transform between the given two CRS
   * @param sourceSRID the SRID of the source coordinate reference system (EPSG:sourceSRID)
   * @param targetSRID the SRID of the target coordinate reference system (EPSG:sourceSRID)
   * @param sparkConf used to retrieve [[CRSServer]] address and port to convert the SRID to CRS
   * @return the math transformation that transforms from source to destination
   */
  def findTransformationInfo(sourceSRID: Int, targetSRID: Int, sparkConf: SparkConf): TransformationInfo = {
    val key = s"$sourceSRID/$targetSRID"
    CachedTransformationInfo.getOrElseUpdate(key, {
      val sourceCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(sourceSRID, sparkConf)
      val targetCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(targetSRID, sparkConf)
      val transform: MathTransform = CRS.findMathTransform(sourceCRS, targetCRS, true)
      TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID, transform,
        GeometryReader.getGeometryFactory(targetSRID))
    })
  }

  /**
   * Creates or retrieves a cached math transform to transform between the given two CRS
   * @param sourceSRID the SRID of the source coordinate reference system (EPSG:sourceSRID)
   * @param targetCRS target coordinate reference system
   * @param sparkConf used to retrieve [[CRSServer]] address and port to convert between SRID and CRS
   * @return the math transformation that transforms from source to destination
   */
  def findTransformationInfo(sourceSRID: Int, targetCRS: CoordinateReferenceSystem, sparkConf: SparkConf): TransformationInfo = {
    val targetSRID: Integer = CRS.lookupEpsgCode(targetCRS, false)
    if (targetSRID == null) {
      val sourceCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(sourceSRID, sparkConf)
      val transform: MathTransform = CRS.findMathTransform(sourceCRS, targetCRS, true)
      return TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID,
        transform, GeometryReader.getGeometryFactory(targetSRID))
    }
    val key = s"$sourceSRID/$targetSRID"
    CachedTransformationInfo.getOrElseUpdate(key, {
      val sourceCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(sourceSRID, sparkConf)
      val transform: MathTransform = CRS.findMathTransform(sourceCRS, targetCRS, true)
      TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID,
        transform, GeometryReader.getGeometryFactory(targetSRID))
    })
  }

  /**
   * Reprojects the given geometry from source to target CRS. This method ignores the SRID of the geometry and
   * assumes it to be in the source CRS.
   * @param geometry the geometry to transform
   * @param sourceCRS source coordinate reference system
   * @param targetCRS target coordinate reference system
   * @param sparkConf used to retrieve [[CRSServer]] address and port to convert between SRID and CRS
   * @return a new geometry that is transformed
   */
  def reprojectGeometry(geometry: Geometry, sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem, sparkConf: SparkConf): Geometry =
    reprojectGeometry(geometry, findTransformationInfo(sourceCRS, targetCRS, sparkConf))

  /**
   * Reprojects the given geometry to target CRS. This method uses the SRID in the given geometry to determine
   * the source coordinate reference system. If the SRID is invalid, this method will fail.
   * @param geometry the geometry to transform
   * @param targetCRS target coordinate reference system
   * @param sparkConf used to retrieve [[CRSServer]] address and port to convert between SRID and CRS
   * @return a new geometry that is transformed
   */
  def reprojectGeometry(geometry: Geometry, targetCRS: CoordinateReferenceSystem, sparkConf: SparkConf): Geometry =
    reprojectGeometry(geometry, CRSServer.sridToCRS(geometry.getSRID, sparkConf), targetCRS, sparkConf)

  /**
   * Converts the given geometry
   *
   * @param geometry the geometry to reproject to the target CRS
   * @param transform the transformation to apply on the given geometry
   * @return the same geometry or a new geometry if it could not be reprojected in place.
   * @throws TransformException if an error happens while transforming the geometry.
   */
  @throws[TransformException]
  def reprojectGeometry(geometry: Geometry,
                        transform: TransformationInfo): Geometry = { // Convert the geometry inplace
    if (transform == null || geometry.isEmpty) return geometry
    var tmpCoords: Array[Double] = null
    val sourceReversed: Boolean = CRS.getAxisOrder(transform.sourceCRS) == AxisOrder.NORTH_EAST
    val targetReversed: Boolean = CRS.getAxisOrder(transform.targetCRS) == AxisOrder.NORTH_EAST
    geometry.getGeometryType match {
      case "Point" =>
        val c = new Coordinate(geometry.getCoordinate)
        tmpCoords = if (!sourceReversed)
          Array[Double](c.getX, c.getY)
        else
          Array[Double](c.getY, c.getX)
        transform.mathTransform.transform(tmpCoords, 0, tmpCoords, 0, 1)
        if (!targetReversed) {
          c.setX(tmpCoords(0))
          c.setY(tmpCoords(1))
        } else {
          c.setX(tmpCoords(1))
          c.setY(tmpCoords(0))
        }
        return transform.targetFactory.createPoint(c)
      case "Envelope" =>
        val e = geometry.asInstanceOf[EnvelopeND]
        assert(e.getCoordinateDimension == 2, "Transform can only work with two-dimensional data")
        tmpCoords = new Array[Double](e.getCoordinateDimension)
        val tmpCoords2: Array[Double] = new Array[Double](tmpCoords.length)
        for ($d <- tmpCoords.indices) {
          if (!sourceReversed) {
            tmpCoords($d) = e.getMinCoord($d)
            tmpCoords2($d) = e.getMaxCoord($d)
          } else {
            tmpCoords($d) = e.getMinCoord(1-$d)
            tmpCoords2($d) = e.getMaxCoord(1-$d)
          }
        }
        transform.mathTransform.transform(tmpCoords, 0, tmpCoords, 0, 1)
        transform.mathTransform.transform(tmpCoords2, 0, tmpCoords2, 0, 1)
        if (!targetReversed) {
          return new EnvelopeND(transform.targetFactory, tmpCoords, tmpCoords2)
        } else {
          return new EnvelopeND(transform.targetFactory, 2, tmpCoords(1), tmpCoords(0), tmpCoords2(1), tmpCoords2(0))
        }
      case "MultiPoint" | "LineString" | "MultiLineString" | "Polygon" | "MultiPolygon" | "GeometryCollection" =>
        return reprojectGeometryJTS(geometry, transform)
      case _ =>
        throw new RuntimeException(String.format("Cannot reproject geometries of type '%s'", geometry.getGeometryType))
    }
    geometry
  }

  @throws[TransformException]
  protected def reprojectGeometryJTS(geometry: Geometry, transform: TransformationInfo): Geometry = {
    var cs: CoordinateSequence = null
    var tmp: Array[Double] = null
    val sourceReversed: Boolean = CRS.getAxisOrder(transform.sourceCRS) == AxisOrder.NORTH_EAST
    val targetReversed: Boolean = CRS.getAxisOrder(transform.targetCRS) == AxisOrder.NORTH_EAST
    geometry.getGeometryType match {
      case "Point" =>
        val coord = new Coordinate(geometry.asInstanceOf[Point].getCoordinate)
        val tmp = if (!sourceReversed)
          Array[Double](coord.getX, coord.getY)
        else
          Array[Double](coord.getY, coord.getX)
        transform.mathTransform.transform(tmp, 0, tmp, 0, tmp.length / 2)
        if (!targetReversed) {
          coord.setX(tmp(0))
          coord.setY(tmp(1))
        } else {
          coord.setX(tmp(1))
          coord.setY(tmp(0))
        }
        transform.targetFactory.createPoint(coord)
      case "LineString" | "LinearRing" =>
        cs = transform.targetFactory.getCoordinateSequenceFactory.create(geometry.asInstanceOf[LineString].getCoordinateSequence)
        tmp = new Array[Double](2 * cs.size)
        for (iPoint <- 0 until cs.size) {
          if (!sourceReversed) {
            tmp(iPoint * 2) = cs.getX(iPoint)
            tmp(iPoint * 2 + 1) = cs.getY(iPoint)
          } else {
            tmp(iPoint * 2) = cs.getY(iPoint)
            tmp(iPoint * 2 + 1) = cs.getX(iPoint)
          }
        }
        transform.mathTransform.transform(tmp, 0, tmp, 0, tmp.length / 2)
        for (iPoint <- 0 until cs.size) {
          if (!targetReversed) {
            cs.setOrdinate(iPoint, 0, tmp(2 * iPoint))
            cs.setOrdinate(iPoint, 1, tmp(2 * iPoint + 1))
          } else {
            cs.setOrdinate(iPoint, 1, tmp(2 * iPoint))
            cs.setOrdinate(iPoint, 0, tmp(2 * iPoint + 1))
          }
        }
        if (geometry.getGeometryType == "LineString") transform.targetFactory.createLineString(cs)
        else transform.targetFactory.createLinearRing(cs)
      case "Polygon" =>
        val p = geometry.asInstanceOf[Polygon]
        val shell = reprojectGeometryJTS(p.getExteriorRing, transform).asInstanceOf[LinearRing]
        val holes = new Array[LinearRing](p.getNumInteriorRing)
        for (iRing <- 0 until p.getNumInteriorRing) {
          holes(iRing) = reprojectGeometryJTS(p.getInteriorRingN(iRing), transform).asInstanceOf[LinearRing]
        }
        transform.targetFactory.createPolygon(shell, holes)
      case "MultiPoint" =>
        val geometries = new Array[Point](geometry.getNumGeometries)
        for (iGeom <- 0 until geometry.getNumGeometries)
          geometries(iGeom) = reprojectGeometryJTS(geometry.getGeometryN(iGeom), transform).asInstanceOf[Point]
        transform.targetFactory.createMultiPoint(geometries)
      case "MultiLineString" =>
        val geometries = new Array[LineString](geometry.getNumGeometries)
        for (iGeom <- 0 until geometry.getNumGeometries)
          geometries(iGeom) = reprojectGeometryJTS(geometry.getGeometryN(iGeom), transform).asInstanceOf[LineString]
        transform.targetFactory.createMultiLineString(geometries)
      case "MultiPolygon" =>
        val geometries = new Array[Polygon](geometry.getNumGeometries)
        for (iGeom <- 0 until geometry.getNumGeometries)
          geometries(iGeom) = reprojectGeometryJTS(geometry.getGeometryN(iGeom), transform).asInstanceOf[Polygon]
        transform.targetFactory.createMultiPolygon(geometries)
      case "GeometryCollection" =>
        val geometries = new Array[Geometry](geometry.getNumGeometries)
        for (iGeom <- 0 until geometry.getNumGeometries)
          geometries(iGeom) = reprojectGeometryJTS(geometry.getGeometryN(iGeom), transform)
        transform.targetFactory.createGeometryCollection(geometries)
      case _ =>
        throw new RuntimeException("Not supported type " + geometry.getGeometryType)
    }
  }

  /**
   * Reproject an envelope (orthogonal rectangle) to another envelope
   * @param envelope the input envelope to convert
   * @param sourceCRS the source coordinate reference system (CRS)
   * @param targetCRS the target coordinate reference system (CRS)
   * @param sparkConf used to retrieve [[CRSServer]] address and port to convert between SRID and CRS
   * @return the converted envelope
   */
  def reprojectEnvelope(envelope: Envelope,
                        sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem,
                       sparkConf: SparkConf): Envelope = {
    val transform: TransformationInfo = findTransformationInfo(sourceCRS, targetCRS, sparkConf)
    val sourceReversed: Boolean = CRS.getAxisOrder(transform.sourceCRS) == AxisOrder.NORTH_EAST
    val targetReversed: Boolean = CRS.getAxisOrder(transform.targetCRS) == AxisOrder.NORTH_EAST

    val tmpCoords: Array[Double] = new Array[Double](4)
    if (!sourceReversed) {
      tmpCoords(0) = envelope.getMinX
      tmpCoords(1) = envelope.getMinY
      tmpCoords(2) = envelope.getMaxX
      tmpCoords(3) = envelope.getMaxY
    } else {
      tmpCoords(0) = envelope.getMinY
      tmpCoords(1) = envelope.getMinX
      tmpCoords(2) = envelope.getMaxY
      tmpCoords(3) = envelope.getMaxX
    }
    transform.mathTransform.transform(tmpCoords, 0, tmpCoords, 0, 2)
    if (!targetReversed) {
      new Envelope(tmpCoords(0), tmpCoords(2), tmpCoords(1), tmpCoords(3))
    } else {
      new Envelope(tmpCoords(1), tmpCoords(3), tmpCoords(0), tmpCoords(2))
    }
  }
}
