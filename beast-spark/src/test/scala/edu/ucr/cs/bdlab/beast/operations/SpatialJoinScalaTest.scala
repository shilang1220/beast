package edu.ucr.cs.bdlab.beast.operations

import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, EnvelopeNDLite, Feature}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.GeometryFactory
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.indexing.GridPartitioner

@RunWith(classOf[JUnitRunner])
class SpatialJoinScalaTest extends FunSuite with ScalaSparkTest {
  test("SpatialJoinDupAvoidance") {
    val r = List(
      Feature.create(null, new EnvelopeND(new GeometryFactory, 2, 10.0, 10.0, 10.1, 10.1)),
      Feature.create(null, new EnvelopeND(new GeometryFactory, 2, 3.0, 1.0, 5.0, 3.0))
    )
    val s = List(
      Feature.create(null, new EnvelopeND(new GeometryFactory, 2, 2.0, 0.0, 4.0, 2.0))
    )
    val results = SpatialJoin.spatialJoinIntersectsPlaneSweepFeatures(r.toArray, s.toArray,
      new EnvelopeNDLite(2, 2.0, 0.0, 5.0, 3.0), ESJPredicate.Intersects, null)
    assert(results.size == 1)

  }

  test("Distributed Join on loaded indexes") {
    val testFile1 = makeDirCopy("/sjoinr.grid")
    val testFile2 = makeDirCopy("/sjoins.grid")
    val dataset1 = sparkContext.readWKTFile(testFile1.getPath, 0)
    val dataset2 = sparkContext.readWKTFile(testFile2.getPath, 0)
    assert(dataset1.getNumPartitions == 2)
    assert(dataset2.getNumPartitions == 2)
    val joinResults = dataset1.spatialJoin(dataset2, ESJPredicate.Intersects, ESJDistributedAlgorithm.DJ)
    assert(joinResults.getNumPartitions == 2)
  }

  test("Distributed Join on memory-partitioned data") {
    val testFile1 = makeFileCopy("/sjoinr.wkt")
    val testFile2 = makeFileCopy("/sjoins.wkt")
    val dataset1 = sparkContext.readWKTFile(testFile1.getPath, 0)
    val dataset2 = sparkContext.readWKTFile(testFile2.getPath, 0)
    val gridPartitioner = new GridPartitioner(new EnvelopeNDLite(2, 0, 0, 11, 5), Array(2,2))
    val partitioned1 = dataset1.partitionBy(gridPartitioner)
    val partitioned2 = dataset2.partitionBy(gridPartitioner)
    assert(partitioned1.getNumPartitions == 4)
    assert(partitioned2.getNumPartitions == 4)
    val joinResults = partitioned1.spatialJoin(partitioned2, ESJPredicate.Intersects)
    assert(joinResults.getNumPartitions == 4)
  }
}
