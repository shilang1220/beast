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
package edu.ucr.cs.bdlab.raptor

import java.util

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

object RaptorJoin {
  /**
   * Performs a raptor join (Raster+Vector) between the rasters in the given path and the given set
   * of geometric features. Each feature is given a numeric ID that is used to produce the output.
   * The return is a set of tuples with the following attributes:
   * <ol>
   *   <li>Long: The ID of the geometry</li>
   *   <li>Int: The index of the file in the given list of files</li>
   *   <li>Int: The x-coordinate of the pixel</li>
   *   <li>Int: The y-coordinate of the pixel</li>
   *   <li>Float: The value associated with the pixel</li>
   * </ol>
   * @param rasters an array of files to process. The index of each entry in this array will be used to associate
   *                the output results to files.
   * @param vector an RDD of pairs of ID and Feature. The ID is used in the output as a reference to the feature.
   * @return an RDD of tuples that contain (feature ID, raster ID, x, y, value)
   */
  def raptorJoin(rasters: Array[String], vector: RDD[(Long, IFeature)], opts: BeastOptions) : RDD[(Long, Int, Int, Int, Float)] = {
    // intersections represent the following information
    // (Tile ID, (Geometry ID, Y, X1, X2))
    // Create a Hadoop configuration but load it into BeastOptions because Hadoop Configuration is not serializable
    val hadoopConf = new BeastOptions(opts)
    val iEntry = vector.sparkContext.hadoopConfiguration.iterator()
    while (iEntry.hasNext) {
      val entry = iEntry.next()
      hadoopConf.put(entry.getKey, entry.getValue)
    }
    val intersectionsRDD: RDD[(Long, (Long, Int, Int, Int))] = vector.mapPartitions { idFeatures: Iterator[(Long, IFeature)] => {
      val conf = hadoopConf.loadIntoHadoopConf(new Configuration(false))
      val intersectionsList = new scala.collection.mutable.ListBuffer[Intersections]()
      val rasterList = new scala.collection.mutable.ListBuffer[Int]()

      val ids = new util.ArrayList[java.lang.Long]()
      val geoms: Array[Geometry] = idFeatures.map(x => {
        ids.add(x._1)
        x._2.getGeometry
      }).toArray

      for (i <- rasters.indices) {
        val raster = rasters(i)
        val rasterPath = new Path(raster)
        val filesystem = rasterPath.getFileSystem(conf)
        val rasterReader: RasterReader = RasterHelper.createRasterReader(filesystem, rasterPath, conf)
        try {
          val intersections = new Intersections
          intersections.compute(ids, geoms, rasterReader)
          if (intersections.getNumIntersections != 0) {
            intersectionsList.append(intersections)
            rasterList.append(i)
          }
        } finally {
          rasterReader.close()
        }
      }
      if (intersectionsList.isEmpty)
        Seq[(Long, (Long, Int, Int, Int))]().iterator
      else
        new IntersectionsIterator(rasterList.toArray, intersectionsList.toArray)
    }}
    // Same format (Tile ID, (Geometry ID, Y, X1, X2))
    val orderedIntersections: RDD[(Long, (Long, Int, Int, Int))] =
      intersectionsRDD.repartitionAndSortWithinPartitions(new HashPartitioner(intersectionsRDD.getNumPartitions))

    // Result is in the format (GeometryID, (RasterFileID, X, Y, RasterValue))
    orderedIntersections.mapPartitions(x => new PixelIterator(x, rasters, opts.getString(RasterReader.RasterLayerID)))
  }
}
