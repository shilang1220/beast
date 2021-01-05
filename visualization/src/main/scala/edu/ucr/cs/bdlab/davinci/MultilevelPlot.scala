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

import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{JavaSpatialRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EmptyGeometry, EnvelopeNDLite, Feature, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.{IndexHelper, RSGrovePartitioner}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat}
import edu.ucr.cs.bdlab.beast.synopses
import edu.ucr.cs.bdlab.beast.synopses.{AbstractHistogram, HistogramOP, Prefix2DHistogram, Summary}
import edu.ucr.cs.bdlab.beast.util._
import edu.ucr.cs.bdlab.davinci.MultilevelPyramidPlotHelper.TileClass
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.Geometry

import java.io.IOException
import scala.collection.mutable.ArrayBuffer

/**
  * Creates a multilevel visualization using the flat partitioning algorithm. The input is split in a non-spatial way
  * into splits. Each split is visualized as a full pyramid which contains all the tiles but the tiles are not final
  * because they do not cover all the data. Finally, the partial tiles are merged to produce final tile images which
  * are then written to the output.
  */
@OperationMetadata(
  shortName =  "mplot",
  description = "Plots the input file as a multilevel pyramid image",
  inputArity = "1",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat],
    classOf[CommonVisualizationHelper], classOf[MultilevelPyramidPlotHelper])
)
object MultilevelPlot extends CLIOperation with Logging {

  @OperationParam(description = "Write data tiles in the output directory next to images." + "If set to false, data tiles are skipped and a link to the input path is added to the output _visualization.properties", defaultValue = "true") val IncludeDataTiles = "data-tiles"

  /** The width of the tile in pixels */
  @OperationParam(description = "The width of the tile in pixels", defaultValue = "256") val TileWidth = "width"
  /** The height of the tile in pixels */
  @OperationParam(description = "The height of the tile in pixels", defaultValue = "256") val TileHeight = "height"

  /** The number of levels for multilevel plot */
  @OperationParam(description =
"""The total number of levels for multilevel plot.
Can be specified as a range min..max (inclusive of both) or as a single number which indicates the number of levels starting at zero."""",
    defaultValue = "7") val NumLevels = "levels"

  /** Data tile threshold. Any tile that is larger than this size is considered an image tile */
  @OperationParam(description = "Data tile threshold for adaptive multilevel plot. Largest possible size for a data tile in terms of number of bytes", defaultValue = "1m") val DataTileThreshold = "threshold"

  @OperationParam(description = "The maximum size for the histogram used in adaptive multilevel plot", defaultValue = "32m") val MaximumHistogramSize = "MultilevelPlot.MaxHistogramSize"

  @OperationParam(description = "Type of histogram used to classify tiles {simple, euler}", defaultValue = "simple") val HistogramType = "histogramtype"

  /**
   * How many levels to combine together when processing the data. Combining multiple levels can speed up the processing
   * by reducing the number of partitions to process. However, it can increase the memory usage since one partition
   * will contain many tiles to process.
   * In one example, increasing the granularity from 1 to 5 with 20 levels decreased the processing time from
   * 500 to 250 seconds on a 12-node cluster.
   */
  @OperationParam(description = "Granularity of pyramid partitioning. How many levels to combine together while partitioning.", defaultValue = "1") val PartitionGranularity = "Pyramid.Granularity"

  /** Java shortcut */
  def plotFeatures(features: JavaSpatialRDD, minLevel: Int, maxLevel: Int, plotterClass: Class[_ <: Plotter],
                   inputPath: String, outputPath: String, opts: BeastOptions): Unit =
    plotFeatures(features.rdd, minLevel to maxLevel, plotterClass, inputPath, outputPath, opts)

  /**
    * Plots the given features and writes the plot results to the output in a pyramid structure
    * @param features the set of features to plot
    * @param levels the range of levels to plot in the pyramid
    * @param plotterClass the plotter class used to generate the tiles
    * @param inputPath the input path. Used to add a link back to this path when the output is partial.
    * @param outputPath the output path where the tiles will be written.
    * @param opts user options for initializing the plotter and controlling the behavior of the operation
    */
  def plotFeatures(features: SpatialRDD, levels: Range, plotterClass: Class[_ <: Plotter],
                   inputPath: String, outputPath: String, opts: BeastOptions): Unit = {
    // Extract plot parameters
    val mbr = new EnvelopeNDLite(2)
    val maxHistogramSize = opts.getSizeAsBytes(MaximumHistogramSize, 32 * 1024 * 1024)
    val htype = Symbol(opts.getString(HistogramType, "simple").toLowerCase)
    val binSize = htype match {
      case 'simple => 8
      case 'euler => 32
    }
    val gridDimension = MultilevelPyramidPlotHelper.computeHistogramDimension(maxHistogramSize, levels.max, binSize)
    logInfo(s"Creating a histogram with dimensions $gridDimension x $gridDimension")

    var featuresToPlot: SpatialRDD = features
    // Check if the Web Mercator option is enabled
    if (opts.getBoolean(CommonVisualizationHelper.UseMercatorProjection, false)) {
      // Web mercator is enabled

      // Apply mercator projection on all features
      featuresToPlot = features
        .map(f => {
          // Trim all geometries to world boundaries and remove geometries that are completely outside map boundaries
          try {
            val originalGeometry: Geometry = f.getGeometry
            if (CommonVisualizationHelper.MercatorMapBoundariesEnvelope.contains(originalGeometry.getEnvelopeInternal))
              f // Completely contained. Nothing needs to be done
            else if (CommonVisualizationHelper.MercatorMapBoundariesEnvelope.intersects(originalGeometry.getEnvelopeInternal))
              Feature.create(f, originalGeometry.intersection(CommonVisualizationHelper.MercatorMapBoundariesPolygon))
            else // Disjoint
              Feature.create(null, EmptyGeometry.instance)
          } catch {
            case _: Throwable => Feature.create(null, EmptyGeometry.instance)
          }
        })
        // Remove empty geometries
        .filter(f => !f.getGeometry.isEmpty)
        // Reproject to Web Mercator space
        .reproject(CommonVisualizationHelper.MercatorCRS)
      // Enable vertical flip when Mercator projection is in use
      opts.setBoolean(CommonVisualizationHelper.VerticalFlip, true)
      // Enforce an MBR that covers the entire world in Mercator projection
      val worldMBR = new EnvelopeNDLite(Reprojector.reprojectEnvelope(CommonVisualizationHelper.MercatorMapBoundariesEnvelope, DefaultGeographicCRS.WGS84,
        CommonVisualizationHelper.MercatorCRS, features.sparkContext.getConf))
      mbr.set(worldMBR)
    } else {
      // No web mercator. Compute the MBR of the input
      // Compute the MBR to know the bounds of the input region
      logInfo("Computing geometric summary")
      val summary = Summary.computeForFeatures(features)
      mbr.set(summary)
      // Expand the MBR to keep the ratio if needed
      if (opts.getBoolean(CommonVisualizationHelper.KeepRatio, true)) {
        // To keep the aspect ratio, expand the MBR to make it a square
        if (mbr.getSideLength(0) > mbr.getSideLength(1)) {
          val diff = mbr.getSideLength(0) - mbr.getSideLength(1)
          mbr.setMinCoord(1, mbr.getMinCoord(1) - diff / 2.0)
          mbr.setMaxCoord(1, mbr.getMaxCoord(1) + diff / 2.0)
        } else if (mbr.getSideLength(1) > mbr.getSideLength(0)) {
          val diff = mbr.getSideLength(1) - mbr.getSideLength(0)
          mbr.setMinCoord(0, mbr.getMinCoord(0) - diff / 2.0)
          mbr.setMaxCoord(0, mbr.getMaxCoord(0) + diff / 2.0)
        }
      }
    }
    opts.set("mbr", mbr.encodeAsString())
    val featuresToProcess: SpatialRDD = if (features.isSpatiallyPartitioned) {
      featuresToPlot
    } else {
      val partitionOpts = new BeastOptions(opts)
      // Disable balanced partitioning to make it more efficient
      partitionOpts.setBoolean(IndexHelper.BalancedPartitioning, false)
      IndexHelper.partitionFeatures(featuresToPlot, classOf[RSGrovePartitioner], _.getStorageSize, partitionOpts).values
    }
    plotFeatures(featuresToProcess, mbr, levels, htype, gridDimension, plotterClass, inputPath, outputPath, opts)
  }

  /** Java shortcut */
  @throws(classOf[IOException])
  def plotGeometries(geoms: JavaRDD[_ <: Geometry], minLevel: Int, maxLevel: Int, outPath: String,
                     opts: BeastOptions): Unit =
    plotGeometries(geoms.rdd, minLevel to maxLevel, outPath, opts)

  /**
    * Plots the given set of geometries and writes the generated image to the output directory. This function uses the
    * [[edu.ucr.cs.bdlab.davinci.GeometricPlotter]] to plot the geometry of the shapes and if data tiles are written
    * to the output, it will use CSV file format by default.
    * @param geoms the set of geometries to plot
    * @param levels the range of levels to generate
    * @param outPath the path to the output directory
    * @param opts additional options to customize the behavior of the plot
    */
  @throws(classOf[IOException])
  def plotGeometries(geoms: RDD[_ <: Geometry], levels: Range, outPath: String, opts: BeastOptions): Unit = {
    // Convert geometries to features to be able to use the standard function
    val features: SpatialRDD = geoms.map(g => Feature.create(null, g))

    // Set a default output format if not set
    if (opts.get(SpatialFileRDD.InputFormat) == null && opts.get(SpatialOutputFormat.OutputFormat) == null)
      opts.set(SpatialOutputFormat.OutputFormat, "wkt")

    opts.set(NumLevels, s"${levels.min}..${levels.max}")

    plotFeatures(features, levels, classOf[GeometricPlotter], null, outPath, opts)
  }


  /**
    * Plots the given set of features using multilevel plot
    * @param features the set of features to plot
    * @param mbr the minimum bounding rectangle of the input area that needs to be plotter (i.e., the top tile)
    * @param levels the range of levels to generate
    * @param htype either &quot;simple&quot; or &quot;euler&quot; (case insensitive)
    * @param gridDimension the size length of the histogram to build based on the alotted memory space
    * @param plotterClass the class of the plotter to use for plotting
    * @param inputPath the path where the input features were read from (it is supposed to be an indexed input)
    * @param outputPath the path to write the output
    * @param opts user options
    * @throws IOException if an error happened during plot
    */
  def plotFeatures(features: SpatialRDD, mbr: EnvelopeNDLite, levels: Range, htype: Symbol, gridDimension: Int,
                   plotterClass: Class[_ <: Plotter], inputPath: String, outputPath: String, opts: BeastOptions): Unit = {
    val threshold: Long = opts.getSizeAsBytes(DataTileThreshold, 1024 * 1024)
    val writeDataTiles: Boolean = opts.getBoolean(IncludeDataTiles, true)
    val tileWidth: Int = opts.getInt(TileWidth, 256)
    val tileHeight: Int = opts.getInt(TileHeight, 256)
    logInfo(s"Computing a histogram of dimension $gridDimension x $gridDimension")

    // Compute the uniform histogram and convert it to prefix sum for efficient sum of rectangles
    val h: AbstractHistogram = if (threshold == 0) null else
    htype match {
      case 'simple =>
        new Prefix2DHistogram(HistogramOP.computePointHistogramSparse(features, _.getStorageSize, mbr, gridDimension, gridDimension))
      case 'euler =>
        new synopses.PrefixEulerHistogram2D(HistogramOP.computeEulerHistogram(features, _.getStorageSize, mbr, gridDimension, gridDimension))
      case _ =>
        throw new RuntimeException(s"Unsupported histogram type $htype")
    }

    val sc = features.context
    val plotter: Plotter = Plotter.getConfiguredPlotter(plotterClass, opts)
    val bufferSize: Double = plotter.getBufferSize / tileWidth.toDouble

    // 1- Create image tiles (any tile with size > threshold)
    // For efficiency, estimate the deepest level with image tile and limit the partitioning to that level
    val deepestLevelWithImageTile: Int = if (threshold == 0) levels.max else MultilevelPyramidPlotHelper
      .findDeepestTileWithSize(h, threshold)
    val pyramidOfImageTiles = new SubPyramid(mbr, levels.min, Math.min(deepestLevelWithImageTile, levels.max))
    val imageTilePartitioner: PyramidPartitioner = if (h == null)
      new PyramidPartitioner(pyramidOfImageTiles) else
      new PyramidPartitioner(pyramidOfImageTiles, h, threshold + 1, Long.MaxValue)
    imageTilePartitioner.setBuffer(bufferSize)
    if (!imageTilePartitioner.isEmpty) {
      sc.setJobGroup("Image tiles", "Image tiles using flat partitioning")
      val imageTiles: RDD[(Long, Canvas)] = createImageTilesWithFlatPartitioning(features, mbr, plotterClass,
        tileWidth, tileHeight, imageTilePartitioner, opts)
      // Write to the output
      val conf = new Configuration()
      conf.setClass(CommonVisualizationHelper.PlotterClassName, plotterClass, classOf[Plotter])
      imageTiles.saveAsNewAPIHadoopFile(outputPath+"/images", classOf[java.lang.Long], classOf[Canvas],
        classOf[PyramidOutputFormat], opts.loadIntoHadoopConf(conf))
    }

    // 2- Create data tiles if needed using pyramid partitioning
    if (writeDataTiles) {
      if (threshold > 0) {
        // 2- Create all data tiles using pyramid partitioning
        sc.setJobGroup("Data tiles", "Data tiles using pyramid partitioning")
        logInfo(s"Apply pyramid partitioning to generate all data tiles with size at most $threshold")
        val deepestLevelWithDataTile: Int = deepestLevelWithImageTile + 1
        val fullPyramid = new SubPyramid(mbr, levels.min, Math.min(levels.max, deepestLevelWithDataTile))
        val dataTilePartitioner = if (h == null)
          new PyramidPartitioner(fullPyramid)
        else new PyramidPartitioner(fullPyramid, h, threshold, TileClass.DataTile)
        dataTilePartitioner.setBuffer(bufferSize)
        createDataTilesWithPyramidPartitioning(features, dataTilePartitioner, outputPath, opts)
      }
    } else {
      // Store the input path in the user configuration so it gets written to _visualization.properties file
      val outPath: Path = new Path(outputPath)
      val inPath: Path = new Path(inputPath)
      opts.set("data", FileUtil.relativize(inPath, outPath).toString)
    }

    // Combine all subdirectories into one directory
    val outPath: Path = new Path(outputPath)
    val outFS: FileSystem = outPath.getFileSystem(opts.loadIntoHadoopConf(null))
    FileUtil.flattenDirectory(outFS, outPath)
    opts.setClass(CommonVisualizationHelper.PlotterClassName, plotterClass, classOf[Plotter])
    opts.set(NumLevels, rangeToString(levels))
    MultilevelPyramidPlotHelper.writeVisualizationAddOnFiles(outFS, outPath, opts)
  }


  /**
    * Create data tiles for the given set of features using the pyramid partitioning method. First, each record is
    * assigned to all data tiles that it overlaps with. Then, the records are grouped by the tile ID. Finally, the
    * records in each group (i.e., tile ID) are written to a separate file with the naming convention tile-z-x-y.
    * @param features the set of features
    * @param partitioner the pyramid partitioner that defines which tiles to plot
    * @param opts the user-defined options for the visualization command.
    */
  private def createDataTilesWithPyramidPartitioning(features: SpatialRDD, partitioner: PyramidPartitioner,
                                                    output: String, opts: BeastOptions): Unit = {
    // Create a sub-pyramid that represents the tiles of interest
    val mbr = new EnvelopeNDLite(2)
    val partitionedFeatures: RDD[(Long, IFeature)] = features.flatMap(feature => {
      mbr.setEmpty()
      mbr.merge(feature.getGeometry)
      val matchedTiles: Array[Long] = partitioner.overlapPartitions(mbr)
      matchedTiles.map(pid => (pid, feature))
    }).sortByKey()

    // Write the final canvas to the output
    partitionedFeatures.saveAsNewAPIHadoopFile(output + "/data", classOf[java.lang.Long], classOf[IFeature],
      classOf[PyramidOutputFormat], opts.loadIntoHadoopConf(null))
  }

  /**
    * Use flat partitioning to plot all tiles in the given range of zoom levels. First, all the partitions are scanned
    * and a set of partial tiles are created based on the data in each partition. Then, these partial tiles are reduced
    * by tile ID so that all partial images for each tile are merged into one final tile. Finally, the final tiles
    * are written to the output using the naming convention tile-z-x-y
    * @param features the set of features to visualize as an RDD
    * @param featuresMBR the MBR of the features
    * @param plotterClass the class to use as a plotter
    * @param tileWidth the width of each tile in pixels
    * @param tileHeight the height of each tile in pixels
    * @param partitioner the pyramid partitioner that selects the tiles to plot using flat partitioning
    * @param opts any other used-defined options
    * @return the created tiles as an RDD of TileID and Canvas
   */
  private def createImageTilesWithFlatPartitioning(features: SpatialRDD, featuresMBR: EnvelopeNDLite,
                                                   plotterClass: Class[_ <: Plotter], tileWidth: Int, tileHeight: Int,
                                                   partitioner: PyramidPartitioner, opts: BeastOptions): RDD[(Long, Canvas)] = {
    val partitionerBroadcast = features.sparkContext.broadcast(partitioner)
    // Assign each feature to all the overlapping tiles and group by tile ID
    val featuresAssignedToTiles: RDD[(Long, IFeature)] = features.flatMap(feature => {
      val mbr = new EnvelopeNDLite(2)
      val allMatches: ArrayBuffer[(Long, IFeature)] = new ArrayBuffer[(Long, IFeature)]
      mbr.merge(feature.getGeometry)
      val matchedPartitions: Array[Long] = partitionerBroadcast.value.overlapPartitions(mbr)
      matchedPartitions.map(tileid => (tileid, feature))
    })
    val plotter: Plotter = Plotter.getConfiguredPlotter(plotterClass, opts)
    val partialTiles: RDD[(Long, Canvas)] = featuresAssignedToTiles.mapPartitions(partitionedFeatures =>
      new TileCreatorFlatPartiotioning(partitionedFeatures, featuresMBR, plotter, tileWidth, tileHeight))

    val finalTiles: RDD[(Long, Canvas)] = partialTiles.reduceByKey((c1, c2) => {
      plotter.merge(c1, c2)
      c1
    })
    finalTiles
  }

  /**
    * Use pyramid partitioning to plot all the images in the given range of zoom levels. First, this method scans all
    * the features and assigns each feature to all image tiles that it overlaps with in the given range of zoom levels.
    * Then, the records are grouped by tile ID and the records in each group (i.e., tile ID) are plotted. Finally,
    * the created image tiles are written to the output using the naming convention tile-z-x-y.
    * This method should be used when the size of each image tile is small enough so that all records in a tile
    * can be grouped together in memory before plotting.
    * @param features the set of features to plot as RDD
    * @param featuresMBR the MBR of the features
    * @param plotterClass the class of the plotter to use
    * @param tileWidth the width of each tile in pixels
    * @param tileHeight the height of each tile in pixels
    * @param partitioner the pyramid partitioner that chooses which tiles to plot
    * @param opts user-defined options
    * @return an RDD of TileID and the canvas
    */
  private[bdlab] def createImageTilesWithPyramidPartitioning(features: SpatialRDD, featuresMBR: EnvelopeNDLite,
                                                             plotterClass: Class[_ <: Plotter], tileWidth: Int,
                                                             tileHeight: Int, partitioner: PyramidPartitioner,
                                                             opts: BeastOptions): RDD[(Long, Canvas)] = {
    // Assign each feature to all the overlapping tiles and group by tile ID
    val featuresAssignedToTiles: RDD[(Long, IFeature)] = features.flatMap(feature => {
      val mbr = new EnvelopeNDLite(2)
      mbr.merge(feature.getGeometry)
      val allMatches: Array[Long] = partitioner.overlapPartitions(mbr)
      allMatches.map(pid => (pid, feature))
    })

    val plotter: Plotter = Plotter.getConfiguredPlotter(plotterClass, opts)
    // Repartition and sort within partitions
    val finalCanvases: RDD[(Long, Canvas)] = featuresAssignedToTiles.sortByKey()
      .mapPartitions(sortedFeatures => new TileCreatorPyramidPartitioning(sortedFeatures, partitioner, plotter, tileWidth, tileHeight))
    finalCanvases
  }

  /**
    * A helper function parses a string into range. The string is in the following forms:
    * - 8: Produces the range [0, 8) exclusive of 8, i.e., [0,7]
    * - 3..6' Produces the range [3, 6], inclusive of both 3 and 4
    * - 4...7: Produces the range [4, 7), exclusive of the 7, i.e., [4, 6]
    * @param str the string to parse
    * @return the created range
    */
  def parseRange(str: String): Range = {
    val oneNumber = raw"(\d+)".r
    val inclusiveRange = raw"(\d+)..(\d+)".r
    val exclusiveRange = raw"(\d+)...(\d+)".r
    str match {
      case oneNumber(number) => 0 until number.toInt
      case inclusiveRange(start, end) => start.toInt to end.toInt
      case exclusiveRange(start, end) => start.toInt until end.toInt - 1
      case _ => throw new RuntimeException(s"Unrecognized range format $str. start..end is a supported format")
    }
  }

  def rangeToString(rng: Range): String = {
    if (rng.start == 0) {
      (rng.last + 1).toString
    } else {
      s"${rng.start}..${rng.last}"
    }
  }

  /**
    * Run the main function using the given user command-line options and spark context
    *
    * @param opts user options for the operation
    * @param sc the spark context to use
    * @return
    */
  @throws(classOf[IOException])
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val features: SpatialRDD = sc.spatialFile(inputs(0), opts)
    val levels: Range = parseRange(opts.getString(NumLevels, "7"))
    val plotterClass: Class[_ <: Plotter] = Plotter.getPlotterClass(opts.getString(CommonVisualizationHelper.PlotterName))
    plotFeatures(features, levels, plotterClass, inputs(0), outputs(0), opts)
  }
}
