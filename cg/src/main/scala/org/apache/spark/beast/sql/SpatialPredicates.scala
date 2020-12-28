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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.locationtech.jts.geom.Geometry

/**
 * A spatial predicate tests a single or two geometries
 */
trait ST_AbstractPredicate extends Expression with CodegenFallback {
  val inputExpressions: Seq[Expression]

  val inputArity: Int

  override def children: Seq[Expression] = inputExpressions

  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputExpressions.length == inputArity && inputExpressions.forall(_.dataType == GeometryDataType))
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects $inputArity geometry arguments")
  }

  override def nullable: Boolean = false

  override def foldable: Boolean = inputExpressions.forall(_.foldable)

  override def dataType: DataType = BooleanType

  override def eval(input: InternalRow): Boolean = {
    val geometries: Seq[Geometry] = inputExpressions.map(e => GeometryDataType.getGeometryFromArray(e.eval(input).asInstanceOf[ArrayData]))
    testPredicate(geometries)
  }

  /**
   * Run the predicate between the two given geometries
   * @param left first geometry
   * @param right second geometry
   * @return
   */
  def testPredicate(input: Seq[Geometry]): Boolean
}

// ------------------ Unary predicates --------------------
/**
 * A binary predicate that tests the relationship of two input arguments
 */
trait ST_AbstractUnaryPredicate extends ST_AbstractPredicate {
  override val inputArity: Int = 1

  override def testPredicate(input: Seq[Geometry]): Boolean = testPredicate(input.head)

  def testPredicate(geom: Geometry): Boolean
}

/**
 * Tests if a geometry is valid, e.g., not intersecting itself
 * @param inputExpressions
 */
case class ST_IsValid(override val inputExpressions: Seq[Expression]) extends ST_AbstractUnaryPredicate {
  override def testPredicate(geom: Geometry): Boolean = geom.isValid
}

/**
 * Tests if a geometry is simple, e.g., not intersecting itself
 * @param inputExpressions
 */
case class ST_IsSimple(override val inputExpressions: Seq[Expression]) extends ST_AbstractUnaryPredicate {
  override def testPredicate(geom: Geometry): Boolean = geom.isSimple
}

/**
 * Tests if a geometry is empty, e.g., containing no points at all
 * @param inputExpressions
 */
case class ST_IsEmpty(override val inputExpressions: Seq[Expression]) extends ST_AbstractUnaryPredicate {
  override def testPredicate(geom: Geometry): Boolean = geom.isEmpty
}

/**
 * Tests if a geometry is an orthogonal rectangle
 * @param inputExpressions
 */
case class ST_IsRectangle(override val inputExpressions: Seq[Expression]) extends ST_AbstractUnaryPredicate {
  override def testPredicate(geom: Geometry): Boolean = geom.isRectangle
}

// -------------------- Binary predicates -------------------

/**
 * A binary predicate that tests the relationship of two input arguments
 */
trait ST_AbstractBinaryPredicate extends ST_AbstractPredicate {
  override val inputArity: Int = 2

  override def testPredicate(input: Seq[Geometry]): Boolean = testPredicate(input(0), input(1))

  def testPredicate(left: Geometry, right: Geometry): Boolean
}

/**
 * Tests if two geometries intersect, i.e., are not disjoint
 * @param inputExpressions
 */
case class ST_Intersects(override val inputExpressions: Seq[Expression]) extends ST_AbstractBinaryPredicate {
  override def testPredicate(left: Geometry, right: Geometry): Boolean = left.intersects(right)
}

/**
 * Tests if second geometry is completely contained in the first geometry
 * @param inputExpressions
 */
case class ST_Contains(override val inputExpressions: Seq[Expression]) extends ST_AbstractBinaryPredicate {
  override def testPredicate(left: Geometry, right: Geometry): Boolean = left.contains(right)
}

/**
 * Tests if the two geometries are disjoint
 * @param inputExpressions
 */
case class ST_Disjoint(override val inputExpressions: Seq[Expression]) extends ST_AbstractBinaryPredicate {
  override def testPredicate(left: Geometry, right: Geometry): Boolean = left.disjoint(right)
}

/**
 * Tests if the two geometries overlap
 * @param inputExpressions
 */
case class ST_Overlaps(override val inputExpressions: Seq[Expression]) extends ST_AbstractBinaryPredicate {
  override def testPredicate(left: Geometry, right: Geometry): Boolean = left.overlaps(right)
}

/**
 * Tests if the first geometry covers the second geometry
 * @param inputExpressions
 */
case class ST_Covers(override val inputExpressions: Seq[Expression]) extends ST_AbstractBinaryPredicate {
  override def testPredicate(left: Geometry, right: Geometry): Boolean = left.covers(right)
}

/**
 * Tests if the first geometry is covered by the second geometry
 * @param inputExpressions
 */
case class ST_CoveredBy(override val inputExpressions: Seq[Expression]) extends ST_AbstractBinaryPredicate {
  override def testPredicate(left: Geometry, right: Geometry): Boolean = left.coveredBy(right)
}

/**
 * Tests if the first geometry crosses the second geometry
 * @param inputExpressions
 */
case class ST_Crosses(override val inputExpressions: Seq[Expression]) extends ST_AbstractBinaryPredicate {
  override def testPredicate(left: Geometry, right: Geometry): Boolean = left.crosses(right)
}