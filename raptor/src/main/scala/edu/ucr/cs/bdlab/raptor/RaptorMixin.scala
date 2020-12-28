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

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import org.apache.spark.rdd.RDD

trait RaptorMixin {

  implicit class RaptorMixinOperations(features: SpatialRDD) {
    /**
     * Performs a Raptor join with the given array of raster file.
     * @param rasters a list of raster files to join with
     * @return an RDD with entries in the following format:
     *         - IFeature: One of the input features
     *         - Int: The index of the file from the list of given input files
     *         - Int: The x coordinate of the pixel
     *         - Int: The y coordinate of the pixel
     *         - Float: The measure value from the raster file converted to Float
     */
    def raptorJoin(rasters: Array[String], opts: BeastOptions = new BeastOptions):
      RDD[(IFeature, Int, Int, Int, Float)] = {
      val featuresWithIds: RDD[(Long, IFeature)] = features.zipWithUniqueId().map(x => (x._2, x._1))
      val raptorResults: RDD[(Long, Int, Int, Int, Float)] = RaptorJoin.raptorJoin(rasters, featuresWithIds, opts)
      // Join back with the features to produce the desired result
      raptorResults.map(x => (x._1, x))
        .join(featuresWithIds)
        .map(x => (x._2._2, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5))
    }
  }
}

object RaptorMixin extends RaptorMixin
