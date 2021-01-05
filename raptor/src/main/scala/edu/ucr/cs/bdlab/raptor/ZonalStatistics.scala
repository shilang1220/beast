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
package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.{CSVFeatureWriter, SpatialFileRDD, SpatialOutputFormat, SpatialWriter}
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.io.{IOException, PrintWriter}

@OperationMetadata(shortName = "zs",
  description = "Computes zonal statistics between a vector file and a raster file. Input files (vector, raster)",
  inputArity = "2",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat]))
object ZonalStatistics extends CLIOperation {
  @transient lazy val logger: Log = LogFactory.getLog(getClass)

  @OperationParam(description = "The statistics function to compute {aggregates}", defaultValue = "aggregates")
  val AggregateFunction = "collector"

  @OperationParam(description = "The name or the index of the raster layer to read from the raster file", defaultValue = "0")
  val RasterLayer = "layer"

  def getRasterCRS(rasterPath: Path, opts: BeastOptions): CoordinateReferenceSystem = {
    val rasterFileSystem = rasterPath.getFileSystem(opts.loadIntoHadoopConf(null))
    // Store raster files as a list of string because Hadoop Path is not serializable
    val rasterFiles: Array[String] =
      if (rasterFileSystem.isDirectory(rasterPath)) {
        rasterFileSystem.listStatus(rasterPath, new PathFilter() {
          override def accept(path: Path): Boolean =
            path.getName.toLowerCase().endsWith(".tif") || path.getName.toLowerCase().endsWith(".hdf")
        }).map(p => p.getPath.toString)
      } else {
        Array(rasterPath.toString)
      }
    val rasterReader = RasterHelper.createRasterReader(rasterFileSystem, new Path(rasterFiles(0)), opts.loadIntoHadoopConf(null))
    try {
      rasterReader.getCRS
    } finally {
      rasterReader.close()
    }
  }

  @throws(classOf[IOException])
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Unit = {
    sc.hadoopConfiguration.setInt("mapred.min.split.size", 8388608)
    sc.hadoopConfiguration.setInt("mapred.max.split.size", 8388608)
    val iLayer = opts.getString(RasterLayer, "0")
    opts.set(RasterReader.RasterLayerID, iLayer)
    val rasterFile = inputs(1)
    val rasterCRS = getRasterCRS(new Path(rasterFile), opts)
    val aggregateFunction = Symbol(opts.getString(AggregateFunction, "aggregates"))
    val vectors: RDD[IFeature] = sc.spatialFile(inputs(0), opts.retainIndex(0)).reproject(rasterCRS)
    val collectorClass: Class[_ <: Collector] = aggregateFunction match {
      case 'aggregates => classOf[Statistics]
      case other => throw new RuntimeException(s"Unrecognized aggregate function $other")
    }

    val results: RDD[(IFeature, Collector)] = zonalStats(vectors, rasterFile, iLayer, collectorClass, opts)

    // Write results to the output
    val normalizedResults: SpatialRDD = results.filter(fc => fc._2 != null).map(fc => {
      val statistics: Statistics = fc._2.asInstanceOf[Statistics]
      val schema = StructType(
        fc._1.schema.filterNot(_.dataType == GeometryDataType) ++ Seq(StructField("sum", StringType), StructField("count", StringType),
          StructField("min", StringType), StructField("max", StringType))
      )
      val values: Array[Any] = (0 until fc._1.getNumAttributes).map(i => fc._1.getAttributeValue(i)).toArray[Any] ++
        Array[Any](statistics.sum.mkString(","), statistics.count.mkString(","),
          statistics.min.mkString(","), statistics.max.mkString(","))

      Feature.create(fc._1.getGeometry, values, schema)
    })
    val oFormat = opts.getString(SpatialOutputFormat.OutputFormat, opts.getString(SpatialFileRDD.InputFormat))
    if (opts.get(CSVFeatureWriter.WriteHeader) == null)
      opts.setBoolean(CSVFeatureWriter.WriteHeader, true)
    val bo: BeastOptions = opts
    SpatialWriter.saveFeatures(normalizedResults, oFormat, outputs(0), bo)
  }

  /**
    * Computes zonal statistics between a set of zones (polygons) and a raster file given by its path and a layer in
    * that file. The result is an RDD of pairs of a feature and a collector value
    * @param zones a set of polygons that represent the regions or zones
    * @param rasterInput the path that contains the raster files
    * @param iLayer the name of the layer to read from the raster files
    * @param collectorClass the class that collects the pixel values to compute the statistics
    * @param opts additional user-defined options
    * @return a set of (Feature, Statistics)
    */
  def zonalStats(zones: RDD[IFeature], rasterInput: String, iLayer: String,
                 collectorClass: Class[_ <: Collector], opts: BeastOptions): RDD[(IFeature, Collector)] = {
    val rasterPath: Path = new Path(rasterInput)
    val rasterFileSystem = rasterPath.getFileSystem(opts.loadIntoHadoopConf(null))
    // Store raster files as a list of string because Hadoop Path is not serializable
    val rasterFiles: Array[String] =
      if (rasterFileSystem.isDirectory(rasterPath)) {
        rasterFileSystem.listStatus(new Path(rasterInput), new PathFilter() {
          override def accept(path: Path): Boolean =
            path.getName.toLowerCase().endsWith(".tif") || path.getName.toLowerCase().endsWith(".hdf")
        }).map(p => p.getPath.toString)
      } else {
        Array(rasterPath.toString)
      }

    // Generate a unique ID for each feature
    val idFeatures: RDD[(Long, IFeature)] = zones.zipWithUniqueId().map(x => (x._2, x._1))

    // (Feature ID, (x, y, value))
    val featureValue: RDD[(Long, (Int, Int, Float))] = RaptorJoin.raptorJoin(rasterFiles, idFeatures, opts)
      .map(x => (x._1, (x._3, x._4, x._5)))

    // Compute statistics for each feature
    val zeroValue = collectorClass.newInstance()
    zeroValue.setNumBands(1)
    val featureStats: RDD[(Long, Collector)] = featureValue.aggregateByKey(zeroValue)(
      (u: Collector, v: (Int, Int, Float)) => u.collect(v._1, v._2, Array[Float](v._3)),
      (u1: Collector, u2: Collector) => u1.accumulate(u2)
    )

    // Join back with the original features to put back the entire feature and remove the ID
    idFeatures.join(featureStats).map( kv => kv._2)
  }

  def printUsage(out: PrintWriter): Unit = {
    out.println("zs <vector file> <raster file> layer:<layer> collector:<collector>")
  }
}
