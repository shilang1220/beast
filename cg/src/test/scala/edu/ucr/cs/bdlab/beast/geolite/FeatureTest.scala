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
package edu.ucr.cs.bdlab.beast.geolite

import org.apache.spark.sql.Row
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.Geometry
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FeatureTest extends FunSuite with ScalaSparkTest {

  test("geometry in middle") {
    val feature = new Feature(Row.apply(123.25, "name",
      new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0), "name2"))
    assert(feature.iGeom == 2)
    assert(feature.getNumAttributes == 3)
    assert(feature.getAs[Double](0) == 123.25)
    assert(feature.getAs[String](1) == "name")
    assert(feature.getAs[Geometry](2) == new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
    assert(feature.getAs[String](3) == "name2")
  }

  test("create from a row without geometry") {
    val feature = new Feature(
      Row.apply(123.25, "name"),
      new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
    assert(feature.iGeom == 0)
    assert(feature.getNumAttributes == 2)
    assert(feature.getAs[Geometry](0) == new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
    assert(feature.getAs[Double](1) == 123.25)
    assert(feature.getAs[String](2) == "name")
  }

  test("create from a row with geometry") {
    val feature = new Feature(
      Row.apply(123.25, "name", new PointND(GeometryReader.DefaultGeometryFactory, 2, 5.0, 6.0)),
      new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
    assert(feature.iGeom == 2)
    assert(feature.getNumAttributes == 2)
    assert(feature.getAs[Double](0) == 123.25)
    assert(feature.getAs[String](1) == "name")
    assert(feature.getAs[Geometry](2) == new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
  }
}