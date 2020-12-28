package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{GeometryHelper, IFeature}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Geometry, GeometryFactory}

import java.util
import scala.collection.mutable

@OperationMetadata(
  shortName = "raptor",
  description = "Computes RaptorZS",
  inputArity = "2",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD])
)
object RaptorZonal extends CLIOperation {

  @transient lazy val logger: Log = LogFactory.getLog(getClass)

  @OperationParam(description = "The name or the index of the raster layer to read from the raster file", defaultValue = "0")
  val RasterLayer = "layer"

  def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    sc.hadoopConfiguration.setInt("mapred.min.split.size", 8388608)
    sc.hadoopConfiguration.setInt("mapred.max.split.size", 8388608)
    //sc.defaultParallelism
    //check the input size

    val t0 = System.nanoTime()

    val rasterPath = new Path(inputs(1))
    val rasterFileSystem = rasterPath.getFileSystem(opts.loadIntoHadoopConf(null))
    val rasterFiles: Array[String] =
      if (rasterFileSystem.isDirectory(rasterPath)) {
        rasterFileSystem.listStatus(new Path(inputs(1)), new PathFilter() {
          override def accept(path: Path): Boolean =
            path.getName.toLowerCase().endsWith(".tif") || path.getName.toLowerCase().endsWith(".hdf")
        }).map(p => p.getPath.toString)
      } else {
        Array(inputs(1))
      }

    val features: RDD[IFeature] = sc.spatialFile(inputs(0), opts.retainIndex(0))
    //val features = SpatialReader.readInput(sc, opts, opts.getInput(0), "WKT(1)")
    //val features = SpatialReader.readInput(sc,opts,opts.getInput(0),"Point(1,2)")

    val geometries: RDD[(Long, IFeature)] = features.map(x => (x.getAs("TLID").toString.toLong, x))

    // RDD[(Geometry ID, Pixel Value)]
    val pixelValues: RDD[(Long, Float)] = RaptorJoin.raptorJoin(rasterFiles, geometries, opts)
      .map(x => (x._1, x._5))

    def agg = (accu: Float, v: Float) => {
      Math.min(accu, v) //(accu + v,Math.min(accu,v),Math.max(accu,v))
    }

    def agg1 = (accu: (Float, Int), v: (Float, Int)) => {
      ((accu._1 + v._1), (accu._2 + v._2))
    }

    def median(inputList: List[Float]): Float = {
      val count = inputList.size
      if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        (inputList(l) + inputList(r)).toFloat / 2
      } else
        inputList(count / 2).toFloat
    }

    def mode(inputList: List[Float]): (Float, Float, Float, Float, Float, Int, Float) = {
      var mode = new mutable.HashMap[Float, Int]()
      var max = Float.NegativeInfinity
      var min = Float.PositiveInfinity
      var sum = (0).toFloat
      for (j <- 0 until inputList.size) {
        val value = inputList(j)
        if (value > max)
          max = value
        if (value < min)
          min = value
        sum += value

        if (mode.contains(value)) {
          val x = mode.get(value).get
          mode.getOrElseUpdate(value, x + 1)
        }
        else {
          mode.put(value, 1)
        }
      }


      val count = inputList.size
      var median = Float.NegativeInfinity
      if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        median = (inputList(l) + inputList(r)).toFloat / 2
      } else
        median = inputList(count / 2).toFloat

      (max, min, median, sum, mutable.ListMap(mode.toSeq.sortWith(_._2 > _._2): _*).head._1, count, (sum / count.toFloat))
    }

    def reg1 = (accu: (Double, Double, Double, Double, Double), v: (Double, Double, Double, Double, Double)) => {
      (accu._1 + v._1, accu._2 + v._2, accu._3 + v._3, accu._4 + v._4, accu._5 + v._5)
    }

    val finaloutput = /*pixeloutput.filter(x=>x._2!=(-9999.0)).groupBy(x=>x._1) */ pixelValues.reduceByKey(agg) //,136)
    //val reg = finaloutput.map(x => (x._2._1.x,x._2._1.y,(x._2._1.x*x._2._1.y),(x._2._1.x*x._2._1.x),(x._2._1.y*x._2._1.y)))
    //val result = reg.reduce(reg1)
    //System.out.println(result._1 + "," + result._2 + "," + result._3 + "," + result._4 + "," + result._5)

    //System.out.println(finaloutput.count())
    //System.out.println(finaloutput.map(x=>x._2._2).max())
    //finaloutput.saveAsTextFile(opts.getOutput)

    finaloutput.collectAsMap()
    val t1 = System.nanoTime()
    //System.out.println("Geometry quadsplit: " + (tn-t0)/1E9)
    //System.out.println("Intersection Time: " + (ti-tn)/1E9)
    //System.out.println("shuffle time: "+(ts-ti)/1E9)
    //System.out.println("pixel output time: "+ (tp-ts)/1E9)
    //System.out.println("Aggregation Time: " + (t1-tp)/1E9)

    System.out.println("Total Time:" + (t1 - t0) / 1E9)

  }

  private def createRectangle(minx: Double, miny: Double, maxx: Double, maxy: Double, f: GeometryFactory) = {
    val cs = f.getCoordinateSequenceFactory.create(5, 2)
    cs.setOrdinate(0, 0, minx)
    cs.setOrdinate(0, 1, miny)
    cs.setOrdinate(1, 0, maxx)
    cs.setOrdinate(1, 1, miny)
    cs.setOrdinate(2, 0, maxx)
    cs.setOrdinate(2, 1, maxy)
    cs.setOrdinate(3, 0, minx)
    cs.setOrdinate(3, 1, maxy)
    cs.setOrdinate(4, 0, minx)
    cs.setOrdinate(4, 1, miny)
    f.createPolygon(cs)
  }

  def quadSplit(geometry:Geometry,theta :Int ) : util.ArrayList[Geometry] = {
    val threshold = theta
    assert (GeometryHelper.getCoordinateDimension(geometry) == 2,  "This function only works with 2D geometries")
    val jtsGeometry = geometry
    val parts = new util.ArrayList[Geometry]()
    var numGeomsToCheck = 1
    parts.add(jtsGeometry)
    while (numGeomsToCheck > 0) {
      val geomToCheck = parts.remove(0)
      numGeomsToCheck = numGeomsToCheck -1
      if (geomToCheck.getNumPoints <= threshold) {
        // Already simple. Add to results
        parts.add(geomToCheck)
      } else {
        // A complex geometry, split into four
        val mbr = geomToCheck.getEnvelopeInternal
        val centerx = (mbr.getMinX + mbr.getMaxX) / 2.0
        val centery = (mbr.getMinY + mbr.getMaxY) / 2.0
        // First quadrant
        var quadrant = createRectangle(mbr.getMinX, mbr.getMinY, centerx, centery, geomToCheck.getFactory)
        parts.add(geomToCheck.intersection(quadrant))
        // Second quadrant
        quadrant = createRectangle(centerx, mbr.getMinY, mbr.getMaxX, centery, geomToCheck.getFactory)
        parts.add(geomToCheck.intersection(quadrant))
        // Third quadrant
        quadrant = createRectangle(centerx, centery, mbr.getMaxX, mbr.getMaxY, geomToCheck.getFactory)
        parts.add(geomToCheck.intersection(quadrant))
        // Fourth quadrant
        quadrant = createRectangle(mbr.getMinX, centery, centerx, mbr.getMaxY, geomToCheck.getFactory)
        parts.add(geomToCheck.intersection(quadrant))
        numGeomsToCheck += 4
      }
    }
    // Convert all parts back to lite geometry
    val results = new util.ArrayList[Geometry]()
    for (i <- 0  until parts.size()) {
      results.add(parts.get(i))
    }
    results
  }

}
