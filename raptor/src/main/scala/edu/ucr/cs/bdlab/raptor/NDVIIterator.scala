package edu.ucr.cs.bdlab.raptor

import java.awt.geom.Point2D
import java.io.{File, FileFilter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class NDVIIterator(iterator: Iterator[(Long, (Int, Int, Int, Int))], rasterList: Array[File],rasterList1: Array[String]) extends  Iterator[(/*SPoint2D.Double*/(Double,Double),Float,String)]{

  var X1: Int = -1
  var X2: Int = -2
  var Y: Int = -1
  var PolygonIndex = -1
  var TileID = -1
  var FileID = -1
  var rasterB4: GeoTiffReader = null
  var rasterB5: GeoTiffReader = null
  var date: String = "empty"
  var nextval:(/*Point2D.Double*/(Double,Double),Float, String) = (null,0,null)
  //nextinternal()

  override def hasNext: Boolean = {

    var temp = nextinternal()

    while(temp._2 == 0 && iterator.hasNext)
      temp = nextinternal()

    if(!iterator.hasNext || temp._2.isNaN)
      false
    else
      true


  }//!(nextval._2.isNaN)

  def nextinternal():(/*Point2D.Double*/ (Double,Double),Float, String)={

    if((X1 == -1) && (X2 == -2) && iterator.hasNext){

      val x = iterator.next()
      X1 = x._2._3
      X2 = x._2._4
      Y = x._2._2
      PolygonIndex = x._2._1
      TileID = (x._1 & 0xffffffff).toInt
      val temp = FileID
      FileID = (x._1 >> 32).toInt
      if(temp!=FileID){

        if(rasterB4!=null)
        {
          rasterB4.close()
          rasterB5.close()
        }

        val conf_file = new Configuration()
        val b4 = (rasterList(FileID)).listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = pathname.getName.toLowerCase().endsWith("b4.tif")
        })(0).toString()
        val b5 = (rasterList(FileID)).listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = pathname.getName.toLowerCase().endsWith("b5.tif")
        })(0).toString()
        date = rasterList1(FileID).substring(15,23)
        val geoTiffFileb4 = new Path(b4)
        val geoTiffFileb5 = new Path(b5)
        rasterB4 = new GeoTiffReader()
        rasterB5 = new GeoTiffReader()
        rasterB4.initialize(geoTiffFileb4.getFileSystem(conf_file), geoTiffFileb4, 0)
        rasterB5.initialize(geoTiffFileb5.getFileSystem(conf_file),geoTiffFileb5,0)
      }
    }

    /*if(X1>X2 && !iterator.hasNext)
      {
        nextval = (null,Float.NaN,date)
        return nextval
      }*/
    val pixelValuesB4 = new Array[Float](rasterB4.getNumComponents)
    val pixelValuesB5 = new Array[Float](rasterB5.getNumComponents)
    rasterB4.getPixelValueAsFloat(X1, Y , pixelValuesB4)
    rasterB5.getPixelValueAsFloat(X1, Y, pixelValuesB5)
    X1=X1+1

    while((pixelValuesB4(0) == rasterB4.getFillValue.toFloat || pixelValuesB5(0) == rasterB5.getFillValue.toFloat) && X2 >= X1 )
    {
      rasterB4.getPixelValueAsFloat(X1, Y , pixelValuesB4)
      rasterB5.getPixelValueAsFloat(X1, Y, pixelValuesB5)
      X1 = X1 +1
    }

    val point = new Point2D.Double()
    rasterB4.gridToModel(X1-1,Y,point)
    nextval = ((point.x,point.y),((pixelValuesB5(0)-pixelValuesB4(0))/(pixelValuesB4(0)+pixelValuesB5(0))),date)
   // System.out.println(nextval)
    /*if(pixelValuesB4(0)==rasterB4.getFillValue.toFloat ||  pixelValuesB5(0) == rasterB5.getFillValue.toFloat)
    {

      if(iterator.hasNext)
      {
        X1 = -1
        X2 = -2
        nextinternal()
      }
      else
        {  nextval = (point,Float.NaN,date)}
    }*/

    if (X1 > X2 && !iterator.hasNext) {
      rasterB4.close()
      rasterB5.close()
      //nextval = (point,Float.NaN,date)

    }

    if (X1 > X2 && iterator.hasNext) {
      X1 = -1
      X2 = -2
    }


    nextval
  }

  override def next(): (/*Point2D.Double*/(Double,Double), Float,String) = {

    var currentval = nextval
    //while(nextval._2==0 && iterator.hasNext)
     //{
       //currentval = nextinternal()

     //}
   // if(iterator.hasNext)
     // nextinternal()
   // System.out.println(currentval)
    currentval

  }

}
