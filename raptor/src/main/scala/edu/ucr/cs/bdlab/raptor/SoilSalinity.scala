package edu.ucr.cs.bdlab.raptor

import java.io.{File, FileFilter}
import java.util

import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.util.OperationMetadata
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{HashPartitioner, SparkContext}

@OperationMetadata(
  shortName =  "soilsalinity",
  description = "Computes NDVI",
  inputArity = "2",
  outputArity = "1"
)
object SoilSalinity extends CLIOperation {

  @transient lazy val logger:Log = LogFactory.getLog(getClass)

  def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val t0 = System.nanoTime()
    //opts.set(SpatialInputFormat.TargetCRS,"Albers|WGS 84")
    val features = sc.spatialFile(inputs(0), "shapefile", opts)

    val rasterFiles = new File(inputs(1))

    val broadCastrasterlist = rasterFiles.listFiles()
    val broadCastrasterlist1 = rasterFiles.list()
    //System.out.println("@@@@@@@@@@" + broadCastrasterlist1.length)

    /*val broadCastrasterBand4 = rasterFiles.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = pathname.getName.toLowerCase().endsWith("b4.tif")
    })(0).toString
    val broadCastrasterBand5 = rasterFiles.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = pathname.getName.toLowerCase().endsWith("b5.tif")
    })(0).toString*/

    //compute intersections by reading in the raster metadata
    val intersectionsRDD = features.mapPartitions{ geometry: Iterator[IFeature] => {
      val intersectionsList = new util.ArrayList[Intersections]()
      val rasterList = new util.ArrayList[Int]()
      val conf_file = new Configuration()
      val geometries = geometry.map(x => x.getGeometry).toArray


      for(i <- 0 to broadCastrasterlist.length-1){
        val raster = new GeoTiffReader()
        val b4 = (broadCastrasterlist(i)).listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = pathname.getName.toLowerCase().endsWith("b4.tif")
        })(0).toString

        val geoTiffFile = new Path(b4)
        try{
          raster.initialize(geoTiffFile.getFileSystem(conf_file), geoTiffFile, 0)
          val intersections = new Intersections
          intersections.compute(geometries,raster)
          if(intersections.getNumIntersections!=0)
          {intersectionsList.add(intersections)
            rasterList.add(i)
          }
        }
        finally {
          raster.close()
        }
      }
      val intersectionsIterator = new IntersectionsIterator1(rasterList,intersectionsList)
      intersectionsIterator

    }}

    //System.out.println(intersectionsRDD.count())
    val output = intersectionsRDD.repartitionAndSortWithinPartitions(new HashPartitioner(144))

    //System.out.println(intersectionsRDD.count())



    val pixeloutput = output.mapPartitions{intersections:Iterator[(Long, (Int, Int, Int, Int))]=> {
      new NDVIIterator(intersections,broadCastrasterlist, broadCastrasterlist1)//broadCastrasterBand4.substring(0,broadCastrasterBand4.length-6))
    }
    }

    //System.out.println(pixeloutput.count())
    pixeloutput.coalesce(1).saveAsTextFile(outputs(0))
   //pixeloutput.groupBy(x=>x._1).mapValues(_.map(_._2).mkString("[",",","]")).coalesce(1).saveAsTextFile(opts.getOutput())
   //pixeloutput.groupBy(x=>x._1).mapValues(_.map(_._3).mkString("(",",",")")).coalesce(1).saveAsTextFile(opts.getOutput() + "dates")
    //pixeloutput.coalesce(1).saveAsTextFile(opts.getOutput)
    //pixeloutput.count()
    val t1 = System.nanoTime()
    System.out.println("Total Time:" + (t1-t0)/1E9)


  }

}
