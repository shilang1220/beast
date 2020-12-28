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
package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat}
import edu.ucr.cs.bdlab.beast.util.OperationMetadata
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

@OperationMetadata(
  shortName =  "splot",
  description = "Plots the input file as a single image",
  inputArity = "1",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat],
    classOf[CommonVisualizationHelper], classOf[SingleLevelPlotHelper])
)
object SingleLevelPlot extends CLIOperation with Logging {

  /**
    * Run the main function using the given user command-line options and spark context
    *
    * @param opts user options for configuring the operation
    * @param sc   the Spark context used to run the operation
    * @return an optional result of this operation
    */
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val features: SpatialRDD = sc.spatialFile(inputs(0), opts)
    val plotterName: String = opts.getString(CommonVisualizationHelper.PlotterName)
    val plotterClass: Class[_ <: Plotter] = Plotter.getPlotterClass(plotterName)
    val imageWidth: Int = opts.getInt(SingleLevelPlotHelper.ImageWidth, 1000)
    val imageHeight: Int = opts.getInt(SingleLevelPlotHelper.ImageHeight, 1000)
    import VisualizationMixin._
    features.plotImage(imageWidth, imageHeight, outputs(0), plotterClass, opts)
  }
}
