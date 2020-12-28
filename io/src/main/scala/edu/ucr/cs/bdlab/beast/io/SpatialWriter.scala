/*
 * Copyright 2018 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes._
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, IFeature}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging

/**
  * A helper class for writing the output
  */
object SpatialWriter extends Logging {


  /**Java shortcut*/
  def saveFeatures(features: JavaSpatialRDD,  oFormat: String, outPath: String, opts: BeastOptions): Unit =
    saveFeatures(features.rdd, oFormat, outPath, opts)

  /**
    * Saves the given set of features to the output using the provided output format.
    * @param features the set of features to store to the output
    * @param oFormat the output format to use for writing
    * @param outPath the path to write the output to
    * @param opts user options to configure the writer
    */
  def saveFeatures(features: SpatialRDD,  oFormat: String, outPath: String, opts: BeastOptions): Unit = {
    val featuresAsPairs = features.map((null, _))
    val hadoopConf = opts.loadIntoHadoopConf(new Configuration(features.sparkContext.hadoopConfiguration))
    SpatialOutputFormat.setOutputFormat(hadoopConf, oFormat)
    if (opts.getBoolean(SpatialOutputFormat.OverwriteOutput, false)) {
      val out: Path = new Path(outPath)
      val filesystem: FileSystem = out.getFileSystem(hadoopConf)
      if (filesystem.exists(out))
        filesystem.delete(out, true)
    }
    featuresAsPairs.saveAsNewAPIHadoopFile(outPath, classOf[EnvelopeND], classOf[IFeature], classOf[SpatialOutputFormat], hadoopConf)
  }
}
