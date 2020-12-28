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
package edu.ucr.cs.bdlab.beast.generator
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, EnvelopeNDLite, GeometryType, PointND}
import org.locationtech.jts.geom.Geometry

/**
 * A generator that generates either points directly or boxes around these points
 * @param partition
 */
abstract class PointBasedGenerator(partition: RandomSpatialPartition)
  extends SpatialGenerator(partition) {

  /**The index of the record that will be returned when next is called*/
  var iRecord: Long = 0

  /**The maximum size for boxes to generate*/
  lazy val maxSize: Array[Double] = partition.opts.getString(PointBasedGenerator.MaxSize).split(",").map(_.toDouble)

  /**Return the geometry type to generate*/
  lazy val geometryType: GeometryType = partition.opts
    .getString(PointBasedGenerator.GeometryType, "point").toLowerCase match {
    case "box" => GeometryType.ENVELOPE
    case "point" => GeometryType.POINT
    case other => throw new RuntimeException(s"Unrecognized geometry type '${other}'")
  }

  /**Whether this generator should generate boxes or not*/
  lazy val isBox: Boolean = geometryType == GeometryType.ENVELOPE

  /**Unit square is the input domain*/
  val UnitSquare: EnvelopeNDLite = new EnvelopeNDLite(2, 0, 0, 1, 1)

  /**
   * Generates a point
   * @return the point generated
   */
  def generatePoint: PointND

  /**
   * Generates a box by first generating a point and building a box around it
   * @return
   */
  def generateBox: EnvelopeND = {
    val center: PointND = generatePoint
    val box: EnvelopeND = new EnvelopeND(center.getFactory, center.getCoordinateDimension)
    for (d <- 0 until center.getCoordinateDimension) {
      val size = uniform(0, maxSize(d))
      box.setMinCoord(d, center.getCoordinate(d) - size / 2)
      box.setMaxCoord(d, center.getCoordinate(d) + size / 2)
    }
    box
  }

  /**
   * Generates and returns the next geometry
   *
   * @return the generated geometry
   */
  override def nextGeometry: Geometry = {
    iRecord += 1
    if (isBox) {
      // Generate box and ensure it is contained in the input domain
      var box: EnvelopeND = null
      do {
        box = generateBox
      } while (!UnitSquare.containsEnvelope(box.getEnvelopeInternal))
      box
    } else {
      // Generate a point and ensure it is contained in the input domain
      var point: PointND = null
      do {
        point = generatePoint
      } while (!UnitSquare.containsPoint(point))
      point
    }
  }

  override def hasNext: Boolean = iRecord < partition.cardinality
}

object PointBasedGenerator {
  /**The maximum size for boxes in the range [0.0, 1.0]. Encoded as comma-separated floating-point values*/
  val MaxSize = "maxSize"

  /**The type of geometry to generate, either "point" or "box"*/
  val GeometryType = "geometry"
}