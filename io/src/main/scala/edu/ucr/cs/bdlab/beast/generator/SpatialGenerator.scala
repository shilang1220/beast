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

import java.awt.geom.AffineTransform

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, Feature, IFeature, PointND}
import edu.ucr.cs.bdlab.beast.io.FeatureReader
import org.locationtech.jts.geom.{Geometry, GeometryFactory}

import scala.util.Random

abstract class SpatialGenerator(partition: RandomSpatialPartition)
  extends Iterator[IFeature] {

  /**The random number generator to use with this partition*/
  val random: Random = new Random(partition.seed)

  /**Generate a random value {0, 1} from a bernoulli distribution with parameter p*/
  def bernoulli(p: Double): Int = if (random.nextDouble() < p) 1 else 0

  /**Generate a random value in the range [a, b) from a uniform distribution*/
  def uniform(a: Double, b: Double): Double = (b - a) * random.nextDouble() + a

  /**Generate a random number in the range (-inf, +inf) from a normal distribution*/
  def normal(mu: Double, sigma: Double): Double =
    mu + sigma * Math.sqrt(-2 * Math.log(random.nextDouble())) * Math.sin(2 * Math.PI * random.nextDouble())

  /**Generate a random integer in the range [1, n]*/
  def dice(n: Int): Int = uniform(0, n).toInt + 1

  /**
   * The geometry factory used to generate the geometries
   */
  val geometryFactory: GeometryFactory = FeatureReader.DefaultGeometryFactory

  lazy val affineMatrix: Array[Double] = {
    val matrixStr: String = partition.opts.getString(SpatialGenerator.AffineMatrix)
    if (matrixStr == null)
      null
    else {
      val matrixComponents = matrixStr.split(",").map(_.toDouble)
      assert(matrixComponents.length == partition.dimensions * (partition.dimensions + 1))
      matrixComponents
    }
  }

  /**
   * Generates and returns the next geometry without applying the affine transformation
   * @return the generated geometry
   */
  def nextGeometry: Geometry

  /**
   * Generates the next feature and applies the affine transformation to it
   * @return a feature that wraps the generated geometry
   */
  def next: IFeature = {
    var geometry: Geometry = nextGeometry
    assert(geometry != null)
    if (affineMatrix != null)
      geometry = affineTransform(geometry)
    new Feature(geometry)
  }

  /**
   * Transform the given geometry using the affine transformation of this generator
   * and returns the transformed geometry
   * @param geometry the geometry to transform
   * @return the transformed geometry
   */
  def affineTransform(geometry: Geometry): Geometry = geometry match {
    case p: PointND =>
      val tp: PointND = new PointND(p.getFactory, partition.dimensions)
      val tindex: Int = partition.dimensions  * partition.dimensions
      for (d <- 0 until partition.dimensions) {
        var v = affineMatrix(tindex + d)
        for (k <- 0 until partition.dimensions) {
          v += affineMatrix((k * partition.dimensions) + d) * p.getCoordinate(k)
        }
        tp.setCoordinate(d, v)
      }
      tp
    case e: EnvelopeND =>
      val te: EnvelopeND = new EnvelopeND(e.getFactory, partition.dimensions)
      val tindex: Int = partition.dimensions * partition.dimensions
      for (d <- 0 until partition.dimensions) {
        var v = affineMatrix(tindex + d)
        for (k <- 0 until partition.dimensions) {
          v += affineMatrix((k * partition.dimensions) + d) * e.getMinCoord(k)
        }
        te.setMinCoord(d, v)
        // Adjust max coordinate
        v = affineMatrix(tindex + d)
        for (k <- 0 until partition.dimensions) {
          v += affineMatrix((k * partition.dimensions) + d) * e.getMaxCoord(k)
        }
        te.setMaxCoord(d, v)
      }
      te
  }
}

object SpatialGenerator {
  /**Number of records per partition*/
  val RecordsPerPartition = "recordsPerPartition"

  /**The seed for the random number generator. If left blank, current time is used*/
  val Seed = "seed"

  /**The number of dimensions of generated geometries*/
  val Dimensions = "dimensions"

  /**
   * The affine matrix represented as comma-separated floating-point values.
   * The order of the matrix components is [m00, m10, m01, m11, m02, m12] where:
   * - m00 (sx) the X coordinate scaling element of the 3x3 matrix
   * - m10 (ry) the Y coordinate shearing element of the 3x3 matrix
   * - m01 (rx) the X coordinate shearing element of the 3x3 matrix
   * - m11 (sy) the Y coordinate scaling element of the 3x3 matrix
   * - m02 (tx) the X coordinate translation element of the 3x3 matrix
   * - m12 (ty) the Y coordinate translation element of the 3x3 matrix
   * See [[AffineTransform]]
   */
  val AffineMatrix = "affineMatrix"
}
