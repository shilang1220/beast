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
package edu.ucr.cs.bdlab.beast.indexing

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import edu.ucr.cs.bdlab.beast.cg.{SparkSpatialPartitioner, SpatialPartitioner}
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{JavaPartitionedSpatialRDD, JavaSpatialRDD, PartitionedSpatialRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature, PointND}
import edu.ucr.cs.bdlab.beast.io.SpatialOutputFormat
import edu.ucr.cs.bdlab.beast.synopses._
import edu.ucr.cs.bdlab.beast.util.{IntArray, OperationHelper, OperationParam}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

/**
 * A helper object for creating indexes and partitioning [[SpatialRDD]]s
 */
object IndexHelper extends Logging {
  /**The different ways for specifying the number of partitions*/
  trait PartitionCriterion
  /**The number of partitions is explicitly specified*/
  case object Fixed extends PartitionCriterion
  /**The number of partitions is adjusted so that each partition has a number of features*/
  case object FeatureCount extends PartitionCriterion
  /**The number of partitions is adjusted so that each partition has a specified size*/
  case object Size extends PartitionCriterion

  /**Information that is used to calculated the number of partitions*/
  case class NumPartitions(pc: PartitionCriterion, value: Long)

  /**The type of the global index (partitioner)*/
  @OperationParam(
    description = "The type of the global index",
    required = false,
    defaultValue = "rsgrove"
  )
  val GlobalIndex = "gindex"

  /**Whether to build a disjoint index (with no overlapping partitions)*/
  @OperationParam(
    description = "Build a disjoint index with no overlaps between partitions",
    defaultValue = "false"
  )
  val DisjointIndex = "disjoint"

  /**The size of the synopsis used to summarize the input before building the index*/
  @OperationParam(
    description = "The size of the synopsis used to summarize the input, e.g., 1024, 10m, 1g",
    defaultValue = "10m"
  )
  val SynopsisSize = "synopsissize";

  /**A flag to increase the load balancing by using the histogram with the sample, if possible*/
  @OperationParam(
    description = "Set this option to combine the sample with a histogram for accurate load balancing",
    defaultValue = "true"
  )
  val BalancedPartitioning = "balanced";

  /**The criterion used to calculate the number of partitions*/
  @OperationParam(
    description =
      """The criterion used to compute the number of partitions. It can be one of:
- Fixed(n): Create a fixed number of partitions (n partitions)
- Size(s): Create n partitions such that each partition contains around s bytes
- Count(c): Create n partitions such that each partition contains around c records""",
    defaultValue = "Size(128m)"
  )
  val PartitionCriterionThreshold = "pcriterion";

  // ---- The following set of functions help in creating a partitioner from a SpatialRDD and a partitioner class

  /**
   * Compute number of partitions for a partitioner given the partitioning criterion and the summary of the dataset.
   *
   * @param numPartitions the desired number of partitions
   * @param summary    the summary of the dataset
   * @return the preferred number of partitions
   */
  def computeNumberOfPartitions(numPartitions: NumPartitions, summary: Summary): Int = numPartitions.pc match {
    case Fixed => numPartitions.value.toInt
    case FeatureCount => Math.ceil(summary.numFeatures.toDouble / numPartitions.value).toInt
    case Size => Math.ceil(summary.size.toDouble / numPartitions.value).toInt
  }

  /**
   * (Java shortcut to)
   * Compute number of partitions for a partitioner given the partitioning criterion and the summary of the dataset.
   *
   * @param pcriterion the criterion used to define the number of partitions
   * @param value the value associated with the criterion
   * @param summary    the summary of the dataset
   * @return the preferred number of partitions
   */
  def computeNumberOfPartitions(pcriterion: String, value: Long, summary: Summary): Int = {
    val pc: PartitionCriterion = pcriterion.toLowerCase match {
      case "fixed" => Fixed
      case "count" => FeatureCount
      case "size" => Size
    }
    computeNumberOfPartitions(NumPartitions(pc, value), summary)
  }

  /**
   * Constructs a spatial partitioner for the given features. Returns an instance of the spatial partitioner class
   * that is given which is initialized based on the given features.
   *
   * @param features the features to create the partitioner on
   * @param partitionerClass the class of the partitioner to construct
   * @param numPartitions the desired number of partitions (this is just a loose hint not a strict number)
   * @param sizeFunction a function that calculates the size of each feature for load balancing. Only needed if
   *                     the partition criterion is specified through partition size [[Size]]
   * @return a constructed spatial partitioner
   */
  def createPartitioner(features: SpatialRDD,
                        partitionerClass: Class[_ <: SpatialPartitioner],
                        numPartitions: NumPartitions,
                        sizeFunction: IFeature=>Int,
                        opts: BeastOptions
                       ): SpatialPartitioner = {
    // The size of the synopsis (summary) that will be created
    val synopsisSize = opts.getSizeAsBytes(SynopsisSize, "10m")
    // Whether to generate a disjoint index (if supported)
    val disjoint = opts.getBoolean(DisjointIndex, false)
    // Whether to generate a highly-balanced partitioning using a histogram (if supported)
    val balanced = opts.getBoolean(BalancedPartitioning, true)

    // Calculate the summary
    val t1 = System.nanoTime()
    val result = summarizeDataset(features.filter(f => !f.getGeometry.isEmpty), partitionerClass, synopsisSize, sizeFunction, balanced)
    val histogram: UniformHistogram = result._1
    val sampleCoordinates: Array[Array[Double]] = result._2
    val summary: Summary = result._3

    val t2 = System.nanoTime

    // Now that the input set has been summarized, we can create the partitioner
    val numCells: Int = computeNumberOfPartitions(numPartitions, summary)
    if (numCells == 1) {
      logInfo("Input too small. Creating a cell partitioner with one cell")
      // Create a cell partitioner that contains one cell that represents the entire input
      val universe = new EnvelopeNDLite(summary)
      universe.setInfinite()
      new CellPartitioner(universe)
      // Notice that it might be possible to avoid computing the histogram and sample. However, it is not worth it
      // since this case happens only for small datasets
    } else {
      val spatialPartitioner: SpatialPartitioner = partitionerClass.newInstance
      spatialPartitioner.setup(opts, disjoint)
      val pMetadata = spatialPartitioner.getMetadata
      if (disjoint && !pMetadata.disjointSupported)
        throw new RuntimeException("Partitioner " + partitionerClass + " does not support disjoint partitioning")

      // Construct the partitioner
      val nump: Int = computeNumberOfPartitions(numPartitions, summary)
      spatialPartitioner.construct(summary, sampleCoordinates, histogram, nump)
      val t3 = System.nanoTime
      logInfo(f"Synopses created in ${(t2 - t1) * 1E-9}%f seconds and partitioner '${partitionerClass.getSimpleName}' " +
        f" constructed in ${(t3 - t2) * 1E-9}%f seconds")
      spatialPartitioner
    }
  }

  /**
   * (Java shortcut to)
   * Constructs a spatial partitioner for the given features. Returns an instance of the spatial partitioner class
   * that is given which is initialized based on the given features.
   *
   * @param features the features to create the partitioner on
   * @param partitionerClass the class of the partitioner to construct
   * @param pcriterion the partition criterion {fixed, count, size}
   * @param pvalue the value of partition criterion
   * @param sizeFunction a function that calculates the size of each feature for load balancing. Only needed if
   *                     the partition criterion is specified through partition size [[Size]]
   * @return a constructed spatial partitioner
   */
  def createPartitioner(features: JavaSpatialRDD,
                        partitionerClass: Class[_ <: SpatialPartitioner],
                        pcriterion: String,
                        pvalue: Long,
                        sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int],
                        opts: BeastOptions
                       ): SpatialPartitioner = {
    val pc = pcriterion match {
      case "fixed" => Fixed
      case "count" => FeatureCount
      case "size" => Size
    }
    createPartitioner(features.rdd, partitionerClass, NumPartitions(pc, pvalue), f => sizeFunction.call(f), opts)
  }

  /**
   * Compute up-to three summaries as supported by the partitioner.
   * [[HistogramOP]].Sparse method since the histogram size is usually large.
   * @param features the features to summarize
   * @param partitionerClass the partitioner class to compute the summaries for
   * @param summarySize the total summary size (combined size for sample and histogram)
   * @param sizeFunction the function the calculates the size of each feature (if size is needed)
   * @param balancedPartitioning set to true if balanced partitioning is desired
   * @return the three computed summaries with nulls for non-computed ones
   */
  private[beast] def summarizeDataset(features: SpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                                      summarySize: Long, sizeFunction: IFeature=>Int, balancedPartitioning: Boolean)
      : (UniformHistogram, Array[Array[Double]], Summary) = {
    lazy val sc: SparkContext = features.sparkContext
    import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
    // The summary is always computed
    val summary: Summary = features.summary
    var sampleCoordinates: Array[Array[Double]] = null
    var histogram: UniformHistogram = null

    // Retrieve the construct method to determine the required parameters
    val constructMethod = partitionerClass.getMethod("construct", classOf[Summary],
      classOf[Array[Array[Double]]], classOf[AbstractHistogram], classOf[Int])
    val parameterAnnotations = constructMethod.getParameterAnnotations
    // Determine whether the sample or the histogram (or both) are needed to construct the partitioner
    val sampleNeeded = parameterAnnotations(1).exists(p => p.isInstanceOf[SpatialPartitioner.Required] ||
      p.isInstanceOf[SpatialPartitioner.Preferred])
    val histogramNeeded = parameterAnnotations(2).exists(p => p.isInstanceOf[SpatialPartitioner.Required]) ||
      (balancedPartitioning && parameterAnnotations(2).exists(p => p.isInstanceOf[SpatialPartitioner.Preferred]))

    val numDimensions = summary.getCoordinateDimension

    // If both sample and histogram are required, reduce the size of the synopsis size to accommodate both
    val synopsisSize = if (sampleNeeded && histogramNeeded) summarySize / 2 else summarySize

    if (sampleNeeded) {
      val sampleSize = (synopsisSize / (8 * numDimensions)).toInt
      val samplingRatio: Double = sampleSize.toDouble / summary.numFeatures min 1.0
      logInfo(s"Drawing a sample of roughly $sampleSize with ratio $samplingRatio")
      val samplePoints: Array[PointND] = features.sample(false, samplingRatio)
        .map(f => new PointND(f.getGeometry))
        .collect()
      sampleCoordinates = Array.ofDim[Double](numDimensions, samplePoints.length)
      for (i <- samplePoints.indices; d <- 0 until numDimensions)
        sampleCoordinates(d)(i) = samplePoints(i).getCoordinate(d)
    }
    // The histogram is computed in another round using the sparse method to reduce the shuffle size
    if (histogramNeeded) {
      // Now, compute the histogram in one pass since the MBR is already calculated
      val numBuckets = (synopsisSize / 8).toInt
      histogram = HistogramOP.computePointHistogramSparse(features, sizeFunction, summary, numBuckets)
    }

    (histogram, sampleCoordinates, summary)
  }

  /**
   * Parse the partition criterion and value in the form "method(value)"
   * @param criterionValue a user-given string in the form "method(value)"
   * @return the parsed partition criterion and value
   */
  def parsePartitionCriterion(criterionValue: String): NumPartitions = {
    val pCriterionRegexp = raw"(fixed|count|size)+\((\w+)\)".r
    criterionValue.toLowerCase match {
      case pCriterionRegexp(method, value) => {
        val pc = method match {
          case "fixed" => Fixed
          case "count" => FeatureCount
          case "size" => Size
        }
        val pvalue: Long = StringUtils.TraditionalBinaryPrefix.string2long(value)
        NumPartitions(pc, pvalue)
      }
    }
  }

  // ---- The following set of functions partition and RDD to generate a partitioned RDD using a partitioner instance

  /**
   * An internal method for partitioning a set of features
   * @param features
   * @param spatialPartitioner
   * @return
   */
  private[beast] def _partitionFeatures(features: SpatialRDD, spatialPartitioner: SpatialPartitioner): PartitionedSpatialRDD = {
    val mbr: EnvelopeNDLite = new EnvelopeNDLite(spatialPartitioner.getCoordinateDimension)
    if (!spatialPartitioner.isDisjoint) {
      // Non disjoint partitioners are easy as each feature is assigned to exactly one partition
      features.map(f => {
        mbr.setEmpty()
        (spatialPartitioner.overlapPartition(mbr.merge(f.getGeometry)), f)
      })
    } else {
      // Disjoint partitioners need us to create a list of partition IDs for each record
      features.flatMap(f => {
        val matchedPartitions = new IntArray
        mbr.setEmpty()
        mbr.merge(f.getGeometry)
        spatialPartitioner.overlapPartitions(mbr, matchedPartitions)
        val resultingPairs = Array.ofDim[(Int, IFeature)](matchedPartitions.size())
        for (i <- 0 until matchedPartitions.size())
          resultingPairs(i) = (matchedPartitions.get(i), f)
        resultingPairs
      })
    }
  }

  /**
    * Partitions the given features using an already initialized [[SpatialPartitioner]].
    *
    * @param features the features to partition
    * @param spatialPartitioner the spatial partitioner to partition the features with
    * @return an RDD of (partition number, IFeature)
    */
  def partitionFeatures(features: SpatialRDD, spatialPartitioner: SpatialPartitioner): PartitionedSpatialRDD = {
    val partitionIDFeaturePairs = _partitionFeatures(features, spatialPartitioner)
    // Enforce the partitioner to shuffle records by partition ID
    partitionIDFeaturePairs.partitionBy(new SparkSpatialPartitioner(spatialPartitioner))
  }

  /**
   * Partition features using an already initialized [[SpatialPartitioner]] from Java
   *
   * @param features the set of features to partition
   * @param partitioner an already initialized partitioner
   * @return a JavaPairRDD where the key represents the partition number and the value is the feature.
   */
  def partitionFeatures(features: JavaSpatialRDD, partitioner: SpatialPartitioner): JavaPairRDD[Integer, IFeature] = {
    val pairs: RDD[(Integer, IFeature)] = IndexHelper
      ._partitionFeatures(features.rdd, partitioner)
      .map(kv => (kv._1, kv._2))
    JavaPairRDD.fromRDD(pairs.partitionBy(new SparkSpatialPartitioner(partitioner)))
  }

  // ---- The following set of functions partition a SpatialRDD given a partitioner class

  /**
   * Partitions the given features using a partitioner of the given type. This method first initializes the partitioner
   * and then uses this initialized partitioner to partition the data.
   *
   * @param features         the RDD of features to partition
   * @param partitionerClass the partitioner class to use for partitioning
   * @param opts             any user options to use while creating the partitioner
   */
  def partitionFeatures(features: SpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                        sizeFunction: IFeature=>Int, opts: BeastOptions): PartitionedSpatialRDD = {
    val pInfo = parsePartitionCriterion(opts.getString(IndexHelper.PartitionCriterionThreshold, "Size(128m)"))
    val spatialPartitioner = createPartitioner(features, partitionerClass, pInfo, sizeFunction, opts)
    partitionFeatures(features, spatialPartitioner)
  }

  /**
   * (Java shortcut to)
   * Partitions the given features using a partitioner of the given type. This method first initializes the partitioner
   * and then uses this initialized partitioner to partition the data.
   *
   * @param features         the RDD of features to partition
   * @param partitionerClass the partitioner class to use for partitioning
   * @param opts             any user options to use while creating the partitioner
   */
  def partitionFeatures(features: JavaSpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                        sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int], opts: BeastOptions)
      : JavaPartitionedSpatialRDD = {
    val pInfo = parsePartitionCriterion(opts.getString(IndexHelper.PartitionCriterionThreshold, "Size(128m)"))
    val spatialPartitioner = createPartitioner(features.rdd, partitionerClass, pInfo, f => sizeFunction.call(f), opts)
    partitionFeatures(features, spatialPartitioner)
  }


  // ----- The following functions serializes and deserializes a partitioner to a Hadoop configuration to use
  // ----- with HadoopOutputFormat to write indexes
  /** Configuration names to store the partitioner into the distributed cache of Hadoop */
  val PartitionerClass = "Partitioner.Class"
  val PartitionerValue = "Partitioner.Value"

  /**
   * Stores the given partitioner to the distributed cache of Hadoop. This should be used when writing the index to
   * the output to give {@link IndexOutputFormat} access to the partitioner.
   *
   * @param hadoopConf  the hadoop configuration to write the partitioner in
   * @param partitioner the partitioner instance
   * @throws IOException if an error happens while writing the file
   */
  @throws[IOException]
  def savePartitionerToHadoopConfiguration(hadoopConf: Configuration, partitioner: SpatialPartitioner): Unit = {
    var tempFile: Path = null
    val fs = FileSystem.get(hadoopConf)
    do {
      tempFile = new Path("spatialPartitioner_"+(Math.random()*1000000).toInt);
    } while (fs.exists(tempFile))
    val out = new ObjectOutputStream(fs.create(tempFile))
    partitioner.writeExternal(out)
    out.close()
    fs.deleteOnExit(tempFile)
    hadoopConf.setClass(PartitionerClass, partitioner.getClass, classOf[SpatialPartitioner])
    hadoopConf.set(PartitionerValue, tempFile.toString)
  }

  /**
   * Retrieves the value of a partitioner for a given job.
   *
   * @param hadoopConf the hadoop configuration to read the partitioner from
   * @return an instance of the partitioner
   */
  def readPartitionerFromHadoopConfiguration(hadoopConf: Configuration): SpatialPartitioner = {
    val klass = hadoopConf.getClass(PartitionerClass, classOf[SpatialPartitioner], classOf[SpatialPartitioner])
    if (klass == null) throw new RuntimeException("PartitionerClass is not set in Hadoop configuration")
    try {
      val partitioner = klass.newInstance
      val partitionerFile = new Path(hadoopConf.get(PartitionerValue))
      val in = new ObjectInputStream(partitionerFile.getFileSystem(hadoopConf).open(partitionerFile))
      partitioner.readExternal(in)
      in.close()
      partitioner
    } catch {
      case e: InstantiationException =>
        throw new RuntimeException("Error instantiating partitioner", e)
      case e: IllegalAccessException =>
        throw new RuntimeException("Error instantiating partitioner", e)
      case e: IOException =>
        throw new RuntimeException("Error retrieving partitioner value", e)
      case e: ClassNotFoundException =>
        throw new RuntimeException("Error retrieving partitioner value", e)
    }
  }

  // ---- The following functions provides access to the set of configured partitioners
  /** A table of all the partitioners available */
  lazy val partitioners: Map[String, Class[_ <: SpatialPartitioner]] = {
    val ps: scala.collection.mutable.TreeMap[String, Class[_ <: SpatialPartitioner]] =
      new scala.collection.mutable.TreeMap[String, Class[_ <: SpatialPartitioner]]()

    val partitionerClasses: java.util.List[String] = OperationHelper.readConfigurationXML("beast.xml").get("SpatialPartitioners")
    val partitionerClassesIterator = partitionerClasses.iterator()
    while (partitionerClassesIterator.hasNext) {
      val partitionerClassName = partitionerClassesIterator.next()
      try {
        val partitionerClass = Class.forName(partitionerClassName).asSubclass(classOf[SpatialPartitioner])
        val metadata = partitionerClass.getAnnotation(classOf[SpatialPartitioner.Metadata])
        if (metadata == null)
          logWarning(s"Skipping partitioner '${partitionerClass.getName}' without a valid Metadata annotation")
        else
          ps.put(metadata.extension, partitionerClass)
      } catch {
        case e: ClassNotFoundException =>
          e.printStackTrace()
      }
    }
    ps.toMap
  }

  import scala.collection.convert.ImplicitConversionsToJava._
  /**
   * (Java shortcut to) Return the set of partitioners defined in the configuration files.
   */
  def getPartitioners: java.util.Map[String, Class[_ <: SpatialPartitioner]] = partitioners

  /**
   * (Java shortcut to) Save a partitioner dataset as a global index file to disk
   *
   * @param partitionedFeatures features that are already partitioned using a spatial partitioner
   * @param path path to the output file to be written
   * @param opts any additional user options
   */
  def saveIndex(partitionedFeatures: JavaPartitionedSpatialRDD, path: String, opts: BeastOptions): Unit = {
    // Could not call the Scala method because the input key is Integer while the scala method expects Int
    // Mapping the input features would not work because the spatial partitioner will be lost
    if (partitionedFeatures.rdd.partitioner.isEmpty)
      throw new RuntimeException("Cannot save non-partitioned features")
    if (!partitionedFeatures.partitioner.get.isInstanceOf[SparkSpatialPartitioner])
      throw new RuntimeException("Can only save features that are spatially partitioner")
    val spatialPartitioner = partitionedFeatures.partitioner.get.asInstanceOf[SparkSpatialPartitioner].getSpatialPartitioner
    val hadoopConf = opts.loadIntoHadoopConf(new Configuration)
    IndexHelper.savePartitionerToHadoopConfiguration(hadoopConf, spatialPartitioner)
    if (opts.getBoolean(SpatialOutputFormat.OverwriteOutput, false)) {
      val out: Path = new Path(path)
      val filesystem: FileSystem = out.getFileSystem(hadoopConf)
      if (filesystem.exists(out))
        filesystem.delete(out, true)
    }
    partitionedFeatures.saveAsNewAPIHadoopFile(path, classOf[Any], classOf[IFeature], classOf[IndexOutputFormat], hadoopConf)
  }

  /**
    * Save a partitioner dataset as a global index file to disk
    *
    * @param partitionedFeatures features that are already partitioned using a spatial partitioner
    * @param path path to the output file to be written
    * @param opts any additional user options
    */
  def saveIndex(partitionedFeatures: RDD[(Int, IFeature)], path: String, opts: BeastOptions): Unit = {
    if (partitionedFeatures.partitioner.isEmpty)
      throw new RuntimeException("Cannot save non-partitioned features")
        if (!partitionedFeatures.partitioner.get.isInstanceOf[SparkSpatialPartitioner])
      throw new RuntimeException("Can only save features that are spatially partitioner")
    val spatialPartitioner = partitionedFeatures.partitioner.get.asInstanceOf[SparkSpatialPartitioner].getSpatialPartitioner
    val hadoopConf = opts.loadIntoHadoopConf(new Configuration)
    IndexHelper.savePartitionerToHadoopConfiguration(hadoopConf, spatialPartitioner)
    if (opts.getBoolean(SpatialOutputFormat.OverwriteOutput, false)) {
      val out: Path = new Path(path)
      val filesystem: FileSystem = out.getFileSystem(hadoopConf)
      if (filesystem.exists(out))
        filesystem.delete(out, true)
    }
    partitionedFeatures.saveAsNewAPIHadoopFile(path, classOf[Any], classOf[IFeature], classOf[IndexOutputFormat], hadoopConf)
  }
}
