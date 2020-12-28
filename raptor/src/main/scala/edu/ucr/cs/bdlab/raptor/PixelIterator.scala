package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import org.apache.hadoop.fs.Path

/**
 * Converts a list of intersections to a list of pixel values in the following format:
 * 1. Long: The feature (or geometry) ID
 * 2. Int: the raster file ID
 * 3. Int: x-coordinate
 * 4. Int: y-coordinate
 * 5. Float: pixel value
 * @param intersections an iterator of intersections in the following format
 *                      (FileID_TileID, (GeometryID, Y, X1, X2))
 * @param rasterFiles a list of raster file names *
 */
class PixelIterator(intersections: Iterator[(Long, (Long, Int, Int, Int))],
                    rasterFiles: Array[String], rasterLayer: String) extends Iterator[(Long, Int, Int, Int, Float)]{

  /**The raster file that nextTuple points to*/
  var currentRasterFileID: Int = -1

  /**The raster file that is currently being read*/
  var rasterFile: RasterReader = _

  /**The range that nextTuple is contained in*/
  var currentRange: (Long, (Long, Int, Int, Int)) = _

  /**The x-coordinate that nextTuple contains*/
  var x: Int = _

  /**The tuple that will be returned when next is called*/
  var nextTuple: (Long, Int, Int, Int, Float) = _

  /**A flag that is raised when end-of-file is reached*/
  var eof: Boolean = false

  /**
   * Prefetches the next tuple and returns it. If end-of-file is reached, this function will return null
   * @return the next record or null if end-of-file is reached
   */
  private def prefetchNext: (Long, Int, Int, Int, Float) = {
    while (!eof) {
      x += 1
      if (currentRange == null || x > currentRange._2._4) {
        // First time or current range has ended, fetch next range
        if (!intersections.hasNext) {
          eof = true
          return null
        }
        currentRange = intersections.next()
        x = currentRange._2._3
        // Open the raster file if needed
        val newRasterFileID: Int = (currentRange._1 >>> 32).toInt
        if (currentRasterFileID != newRasterFileID) {
          currentRasterFileID = newRasterFileID
          // Open a new raster file
          val conf_file = new BeastOptions()
          if (rasterLayer != null)
            conf_file.set(RasterReader.RasterLayerID, rasterLayer)
          val rasterPath = new Path(rasterFiles(currentRasterFileID))
          rasterFile = RasterHelper.createRasterReader(rasterPath.getFileSystem(conf_file.loadIntoHadoopConf(null)), rasterPath, conf_file.loadIntoHadoopConf(null))
        }
      }

      if (!rasterFile.isFillValue(x, currentRange._2._2)) {
        // Found a valid pixel, return it
        val pixelValueFloat = new Array[Float](rasterFile.getNumComponents)
        rasterFile.getPixelValueAsFloat(x, currentRange._2._2, pixelValueFloat)
        return (currentRange._2._1, currentRasterFileID, x, currentRange._2._2, pixelValueFloat(0))
      }
    }
    // End-of-file reached
    null
  }

  nextTuple = prefetchNext

  override def hasNext: Boolean = nextTuple != null

  override def next: (Long, Int, Int, Int, Float) = {
    val toReturn = nextTuple
    nextTuple = prefetchNext
    toReturn
  }

}
