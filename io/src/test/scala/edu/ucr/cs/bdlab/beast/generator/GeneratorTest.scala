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

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeneratorTest extends FunSuite with ScalaSparkTest {
  test("generate uniform data") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100)
    assert(randomPoints.getNumPartitions == 1)
    assert(randomPoints.count() == 100)
    assert(randomPoints.first().getGeometry.getGeometryType == "Point")
  }

  test("Generate multiple partitions") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      SpatialGenerator.RecordsPerPartition -> "53")
    assert(randomPoints.getNumPartitions == 2)
    assert(randomPoints.count() == 100)
    assert(randomPoints.first().getGeometry.getGeometryType == "Point")
  }

  test("Generate boxes") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      Seq(PointBasedGenerator.GeometryType -> "box", PointBasedGenerator.MaxSize -> "0.2,0.1"))
    assert(randomPoints.count() == 100)
    assert(randomPoints.first().getGeometry.getGeometryType == "Envelope")
  }

  test("Generate points with transformation") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      SpatialGenerator.AffineMatrix -> "1,0,0,2,1,2")
    assert(randomPoints.count() == 100)
    val mbr = randomPoints.summary
    assert(new EnvelopeNDLite(2, 1.0, 2.0, 2.0, 4.0).containsEnvelope(mbr))
  }

  test("Generate boxes with transformation") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      Seq(PointBasedGenerator.GeometryType -> "box",
        PointBasedGenerator.MaxSize -> "0.0,0.0",
        SpatialGenerator.AffineMatrix -> "1,0,0,2,1,2")
    )
    assert(randomPoints.count() == 100)
    val mbr = randomPoints.summary
    assert(new EnvelopeNDLite(2, 1.0, 2.0, 2.0, 4.0).containsEnvelope(mbr))
  }

  test("Generate parcel one partition") {
    val randomBoxes: SpatialRDD = new RandomSpatialRDD(sparkContext, ParcelDistribution, 100,
      Seq(ParcelGenerator.Dither -> "0.2", ParcelGenerator.SplitRange -> "0.4")
    )
    assert(randomBoxes.count() == 100)
    val mbr = randomBoxes.summary
    assert(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0).containsEnvelope(mbr))
    // Ensure that all boxes are not overlapping
    val boxes = randomBoxes.collect()
    for (i <- boxes.indices; j <- 0 until i)
      assert(!boxes(i).getGeometry.overlaps(boxes(j).getGeometry))
  }

  test("Generate parcel multiple partition") {
    val randomBoxes: SpatialRDD = new RandomSpatialRDD(sparkContext, ParcelDistribution, 100,
      Seq(ParcelGenerator.Dither -> "0.2", ParcelGenerator.SplitRange -> "0.4",
        SpatialGenerator.RecordsPerPartition -> "53")
    )
    assert(randomBoxes.getNumPartitions == 2)
    assert(randomBoxes.count() == 100)
    // Ensure that all boxes are not overlapping
    val boxes = randomBoxes.collect()
    for (i <- boxes.indices; j <- 0 until i)
      assert(!boxes(i).getGeometry.overlaps(boxes(j).getGeometry))
  }
}
