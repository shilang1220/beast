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
package edu.ucr.cs.bdlab.beast.indexing

import edu.ucr.cs.bdlab.beast.cg.SparkSpatialPartitioner
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.JavaPartitionedSpatialRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature, PointND}
import edu.ucr.cs.bdlab.beast.io.FeatureReader
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexHelperTest extends FunSuite with ScalaSparkTest {
  test("partition features from Java") {
    val geometryFactor: GeometryFactory = FeatureReader.DefaultGeometryFactory
    val features = sparkContext.parallelize(Seq[IFeature](
      new Feature(new PointND(geometryFactor, 2, 0, 0)),
      new Feature(new PointND(geometryFactor, 2, 1, 1)),
      new Feature(new PointND(geometryFactor, 2, 3, 1)),
      new Feature(new PointND(geometryFactor, 2, 1, 4)),
    ))
    val partitionedFeatures: JavaPartitionedSpatialRDD =
      IndexHelper.partitionFeatures(JavaRDD.fromRDD(features), classOf[RSGrovePartitioner],
      new org.apache.spark.api.java.function.Function[IFeature, Int]() {
        override def call(v1: IFeature): Int = 1
      } , new BeastOptions())
    assert(partitionedFeatures.partitioner.isPresent)
    assert(partitionedFeatures.partitioner.get().isInstanceOf[SparkSpatialPartitioner])
  }
}
