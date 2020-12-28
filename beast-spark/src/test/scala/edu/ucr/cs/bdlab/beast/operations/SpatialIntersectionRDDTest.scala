package edu.ucr.cs.bdlab.beast.operations

import java.io.{FileOutputStream, PrintStream}

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, EnvelopeNDLite, Feature}
import edu.ucr.cs.bdlab.beast.indexing.GridPartitioner
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.GeometryFactory
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SpatialIntersectionRDDTest extends FunSuite with ScalaSparkTest {
  test("Join complex partitioned files") {
    val parksFile = makeDirCopy("/parks_index").getPath
    for (i <- 0 to 3) {
      val fileout = new PrintStream(new FileOutputStream("%s/part-%05d.csv".format(parksFile, i)))
      for (j <- 0 to 9)
        fileout.println("EMPTY\t%093d".format(j))
      fileout.close()
    }
    val lakesFile = makeDirCopy("/lakes_index").getPath
    for (i <- 0 to 4) {
      val fileout = new PrintStream(new FileOutputStream("%s/part-%05d.csv".format(lakesFile, i)))
      for (j <- 0 to 9)
        fileout.println("EMPTY\t%093d".format(j))
      fileout.close()
    }
    val parks = sparkContext.spatialFile(parksFile, "wkt")
    val lakes = sparkContext.spatialFile(lakesFile, "wkt")
    val joined = new SpatialIntersectionRDD1(parks, lakes)
    assert(joined.getNumPartitions == 6)
    assert(joined.count() == 6)
  }
}
