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
package org.apache.spark.beast.sql

import edu.ucr.cs.bdlab.beast.geolite.{GeometryReader, PointND}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom.{CoordinateXY, CoordinateXYZM, Envelope, Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader

trait AbstractCreationFunction extends Expression with CodegenFallback {
  /**Geometry factory to create geometries*/
  val geometryFactory: GeometryFactory = GeometryReader.DefaultGeometryFactory

  val inputExpressions: Seq[Expression]

  val inputArity: Int

  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputArity == -1 || inputExpressions.length == inputArity)
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects $inputArity inputs but received ${inputExpressions.length}")
  }

  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = false

  override def foldable: Boolean = inputExpressions.forall(_.foldable)

  override def dataType: DataType = GeometryDataType

  override def eval(input: InternalRow): Any = {
    val inputValues: Seq[Any] = inputExpressions.map(e => e.eval(input))
    val result: Geometry = performFunction(inputValues)
    GeometryDataType.setGeometryInRow(result)
  }

  def performFunction(inputs: Seq[Any]): Geometry
}

/**
 * Creates a point from two or more coordinates.
 * @param inputExpressions
 */
case class ST_CreatePoint(override val inputExpressions: Seq[Expression])
  extends AbstractCreationFunction {
  override val inputArity: Int = -1

  override def performFunction(inputs: Seq[Any]): Geometry = {
    val coordinates: Array[Double] = inputs.map(x => x.asInstanceOf[Number].doubleValue()).toArray
    if (coordinates == 2)
      geometryFactory.createPoint(new CoordinateXY(coordinates(0), coordinates(1)))
    else if (coordinates == 3)
      geometryFactory.createPoint(new CoordinateXYZM(coordinates(0), coordinates(1), coordinates(2), Double.NaN))
    else
      new PointND(geometryFactory, coordinates.length, coordinates:_*)
  }
}

/**
 * Creates a two-dimensional envelope from four coordinates (x1, y1, x2, y2)
 * @param inputExpressions
 */
case class ST_CreateEnvelope(override val inputExpressions: Seq[Expression])
  extends AbstractCreationFunction {

  override val inputArity: Int = 4

  override def performFunction(inputs: Seq[Any]): Geometry = {
    val coordinates: Array[Double] = inputs.map(x => x.asInstanceOf[Number].doubleValue()).toArray
    geometryFactory.toGeometry(new Envelope(coordinates(0), coordinates(2), coordinates(1), coordinates(3)))
  }
}

/**
 * Create a geometry from a WKT representation
 * @param inputExpressions
 */
case class ST_FromWKT(override val inputExpressions: Seq[Expression])
  extends AbstractCreationFunction {

  override val inputArity: Int = 1

  val wktParser = new WKTReader(geometryFactory)

  override def performFunction(inputs: Seq[Any]): Geometry = {
    wktParser.read(inputs.head.asInstanceOf[String])
  }
}