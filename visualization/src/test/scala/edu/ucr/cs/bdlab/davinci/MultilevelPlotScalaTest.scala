package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MultilevelPlotScalaTest extends FunSuite with ScalaSparkTest {

  test("PyramidPartitioningPlot") {
    val inputFile = makeFileCopy("/viztest.point").getPath
    val features: SpatialRDD = sparkContext.readCSVPoint(inputFile, 0, 1, ',')
    val inputMBR = features.summary
    val pyramid = new SubPyramid(inputMBR, 0, 2)
    val opts = new BeastOptions(false)
    opts.setBoolean("vflip", false)
    val tiles: RDD[(Long, Canvas)] = MultilevelPlot.createImageTilesWithPyramidPartitioning(features, inputMBR, classOf[GeometricPlotter],
      256, 256, new PyramidPartitioner(pyramid), opts)
    assert(tiles.count() == 8)
  }

}
