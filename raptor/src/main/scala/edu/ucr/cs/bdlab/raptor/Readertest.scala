package edu.ucr.cs.bdlab.raptor

import java.io.{File, FileFilter}

import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.util.OperationMetadata
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

@OperationMetadata(
  shortName =  "readertest",
  description = "Computes readtime",
  inputArity = "0",
  outputArity = "0"
)
object Readertest extends CLIOperation {

  def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val rasterFiles = new File("/data/Samriddhi/Horus_data/Rasters/LandsatTreecover")
    var broadCastrasterList:Array[File] = null
    if(rasterFiles.isDirectory)
    {
      broadCastrasterList = rasterFiles.listFiles(new FileFilter {
        override def accept(pathname: File): Boolean = pathname.getName.toLowerCase().endsWith(".tif")
      })
    }
    else
    {
      broadCastrasterList = new Array[File](1)
      broadCastrasterList(0) = rasterFiles
    }

    val t0 =System.nanoTime()
    for(i<- 0 until broadCastrasterList.length){
      System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + i)
      val conf_file = new Configuration()
      val raster = new GeoTiffReader()
      val geoTiffFile = new Path(broadCastrasterList(i).getPath)
      try{
        raster.initialize(geoTiffFile.getFileSystem(conf_file), geoTiffFile, 0)

        var x = 0
        var y = 0
        while(y<raster.getRasterHeight)
        {
          for(y1<- y until y + raster.getTileHeight)
          {
            x = 0
            while(x<raster.getRasterWidth){

              val values = new Array[Float](raster.getNumComponents)
              for(x1<- x until x + raster.getTileWidth)
                raster.getPixelValueAsFloat(x1,y1,values)

              x = x + raster.getTileWidth
            }
          }
        y = y + raster.getTileHeight
        }
      }
      finally {
        raster.close()
      }
    }
    val t1 = System.nanoTime()

    System.out.println("Total Time:" + (t1-t0)/1E9)
  }

}
