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
package org.apache.spark.beast

import org.apache.spark.beast.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.types.UDTRegistration
import org.locationtech.jts.geom.Geometry

object SparkSQLRegistration {
  def registerUDT: Unit = {
    // Register the Geometry user-defined data type
    UDTRegistration.register(classOf[Geometry].getName, classOf[GeometryDataType].getName)
  }

  /**
   * Generate the SQL function name from the class
   * @param function the function
   * @return the short name of the class
   */
  def getFunctionName(function: FunctionBuilder): String = function.getClass.getSimpleName.dropRight(1)

  val functions: Seq[FunctionBuilder] = Seq[FunctionBuilder](
    // Unary predicates
    ST_IsValid, ST_IsSimple, ST_IsEmpty, ST_IsRectangle,
    // Binary predicates
    ST_Intersects, ST_Contains, ST_Disjoint, ST_Overlaps, ST_Covers, ST_CoveredBy, ST_Crosses,
    // Unary analysis functions
    ST_ConvexHull, ST_Boundary, ST_Norm, ST_Reverse, ST_Centroid,
    // Binary spatial analysis functions
    ST_Intersection, ST_Union, ST_Difference,
    // Creation functions
    ST_CreatePoint, ST_CreateEnvelope, ST_FromWKT,
  )

  def registerUDF(sparkSession: SparkSession): Unit = {
    functions.foreach(f => sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(getFunctionName(f), f))
  }
}
