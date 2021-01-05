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
package edu.ucr.cs.bdlab.beast.operations

import java.io.IOException
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.{PlaneSweepSpatialJoinIterator, SparkSpatialPartitioner}
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.{CellPartitioner, GridPartitioner}
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat}
import edu.ucr.cs.bdlab.beast.synopses.Summary
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

@OperationMetadata(shortName = "sj",
  description = "Computes spatial join that finds all overlapping features from two files.",
  inputArity = "2",
  outputArity = "?",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat]))
object SpatialJoin extends CLIOperation with Logging {

  @OperationParam(description = "The spatial join algorithm to use {'bnlj', 'dj', 'pbsm' = 'sjmr'}.", defaultValue = "bnlj")
  val SpatialJoinMethod = "method"

  @OperationParam(description = "The spatial predicate to use in the join. Supported predicates are {intersects, contains}", defaultValue = "intersects")
  val SpatialJoinPredicate = "predicate"

  @OperationParam(description = "Overwrite the output file if it exists", defaultValue = "true")
  val OverwriteOutput = "overwrite"

  @OperationParam(description = "Write the output to a file", defaultValue = "true", showInUsage = false)
  val WriteOutput = "output"

  /** The name of the accumulator that records the total number of MBRTests */
  val MBRTestsAccumulatorName = "MBRTests"

  @throws(classOf[IOException])
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Unit = {
    // Set the split size to 16MB so that the code will not run out of memory
    sc.hadoopConfiguration.setInt("mapred.max.split.size", 16 * 1024 * 1024)
    val mbrTests = sc.longAccumulator(MBRTestsAccumulatorName)
    val joinPredicate = opts.getEnumIgnoreCase(SpatialJoinPredicate, ESJPredicate.Intersects)
    val joinMethod = opts.getEnumIgnoreCase(SpatialJoinMethod, ESJDistributedAlgorithm.DJ)
    // Skip duplicate avoidance while reading the input if we run the DJ algorithm
    if (joinMethod == ESJDistributedAlgorithm.DJ)
      opts.setBoolean(SpatialFileRDD.DuplicateAvoidance, false)
    else if (joinMethod == ESJDistributedAlgorithm.REPJ)
      opts.setBoolean(SpatialFileRDD.DuplicateAvoidance+"[0]", false)
    val f1rdd = sc.spatialFile(inputs(0), opts.retainIndex(0))
    val f2rdd = sc.spatialFile(inputs(1), opts.retainIndex(1))
    val joinResults = spatialJoin(f1rdd, f2rdd, joinPredicate, joinMethod, mbrTests)
    if (opts.getBoolean(WriteOutput, true)) {
      val outPath = new Path(outputs(0))
      // Delete output file if exists and overwrite flag is on
      val fs = outPath.getFileSystem(sc.hadoopConfiguration)
      if (fs.exists(outPath) && opts.getBoolean(SpatialOutputFormat.OverwriteOutput, false))
        fs.delete(outPath, true)
      val resultSize = sc.longAccumulator("resultsize")
      joinResults.map(f1f2 => {
        resultSize.add(1)
        f1f2._1.toString + f1f2._2.toString
      }).saveAsTextFile(outputs(0))
      logInfo(s"Join result size is ${resultSize.value}")
    } else {
      // Skip writing the output. Join count
      val resultSize = joinResults.count()
      logInfo(s"Join result size is ${resultSize}")
    }
    logInfo(s"Total number of MBR tests is ${mbrTests.value}")
  }

  /**
   * Performs a spatial join between the given two inputs and returns an RDD of pairs of matching features.
   * This method is a transformation. However, if the [[ESJDistributedAlgorithm.PBSM]] is used, the MBR of the two
   * inputs has to be calculated first which runs a reduce action on each dataset even if the output of the spatial
   * join is not used.
   * @param r1 the first (left) dataset
   * @param r2 the second (right) dataset
   * @param joinPredicate the join predicate. The default is [[ESJPredicate.Intersects]] which finds all non-disjoint
   *                      features
   * @param joinMethod the join algorithm. The default is [[ESJDistributedAlgorithm.BNLJ]] which ignores the index and
   *                   runs a block-nested loop join algorithm.
   * @param mbrCount an (optional) accumulator to count the number of MBR tests during the algorithm.
   * @return an RDD that contains pairs of matching features.
   */
  def spatialJoin(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate = ESJPredicate.Intersects,
                  joinMethod: ESJDistributedAlgorithm = null,
                  mbrCount: LongAccumulator = null): RDD[(IFeature, IFeature)] = {
    val inputsPartitioned: Boolean = r1.isSpatiallyPartitioned && r2.isSpatiallyPartitioned
    // If the joinMethod is not set, choose an algorithm automatically according to the following rules
    val joinAlgorithm = if (joinMethod != null)
      joinMethod
    else if (r1.isSpatiallyPartitioned && r2.isSpatiallyPartitioned)
      ESJDistributedAlgorithm.DJ
    else if (r1.getNumPartitions * r2.getNumPartitions < r1.sparkContext.defaultParallelism)
      ESJDistributedAlgorithm.BNLJ
    else if (r1.isSpatiallyPartitioned || r2.isSpatiallyPartitioned)
      ESJDistributedAlgorithm.REPJ
    else
      ESJDistributedAlgorithm.PBSM

    // Run the spatial join algorithm
    joinAlgorithm match {
      case ESJDistributedAlgorithm.BNLJ =>
        spatialJoinBNLJ(r1, r2, joinPredicate, mbrCount)
      case ESJDistributedAlgorithm.PBSM | ESJDistributedAlgorithm.SJMR =>
        spatialJoinPBSM(r1, r2, joinPredicate, mbrCount)
      case ESJDistributedAlgorithm.DJ if inputsPartitioned =>
        spatialJoinDJIndexedFiles(r1, r2, joinPredicate, mbrCount)
      case ESJDistributedAlgorithm.DJ =>
        spatialJoinBNLJ(r1, r2, joinPredicate, mbrCount)
      case ESJDistributedAlgorithm.REPJ =>
        spatialJoinRepJ(r1, r2, joinPredicate, mbrCount)
      case _other => throw new RuntimeException(s"Unrecognized spatial join method ${_other}. " +
        s"Please specify one of {'pbsm'='sjmr', 'dj', 'repj', 'bnlj'}")
    }
  }

  /**
   * Runs a plane-sweep algorithm between the given two arrays of input features and returns an iterator of
   * pairs of features.
   * @param r the first set of features
   * @param s the second set of features
   * @param dupAvoidanceMBR the duplicate avoidance MBR to run the reference point technique.
   * @param joinPredicate the join predicate to match features
   * @param numMBRTests an (optional) accumulator to count the number of MBR tests
   * @tparam T1 the type of the first dataset
   * @tparam T2 the type of the second dataset
   * @return an iterator over pairs of features
   */
  private[beast] def spatialJoinIntersectsPlaneSweepFeatures[T1 <: IFeature, T2 <: IFeature]
      (r: Array[T1], s: Array[T2], dupAvoidanceMBR: EnvelopeNDLite, joinPredicate: ESJPredicate,
       numMBRTests: LongAccumulator): TraversableOnce[(IFeature, IFeature)] = {
    if (r.isEmpty || s.isEmpty)
      return Seq()
    logInfo(s"Joining ${r.size} x ${s.size} records")
    val refine: ((_ <: IFeature, _ <: IFeature)) => Boolean = joinPredicate match {
      case ESJPredicate.Contains => p => try {p._1.getGeometry.contains(p._2.getGeometry)}
      catch {case e: RuntimeException => logWarning(s"Error comparing records", e); false}
      case ESJPredicate.Intersects => p => try {p._1.getGeometry.intersects(p._2.getGeometry)}
      catch {case e: RuntimeException => logWarning(s"Error comparing records", e); false}
      // For MBR intersects, no refine step is needed. Write the results directly to the output
      case ESJPredicate.MBRIntersects => _ => true
    }
    new PlaneSweepSpatialJoinIterator(r, s, dupAvoidanceMBR, numMBRTests)
      .filter(refine)
  }

  /**
   * Performs a partition-based spatial-merge (PBSM) join as explained in the following paper.
   * Jignesh M. Patel, David J. DeWitt:
   * Partition Based Spatial-Merge Join. SIGMOD Conference 1996: 259-270
   * https://doi.org/10.1145/233269.233338
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @param numMBRTests   (output) the number of MBR tests done during the algorithm
   * @return a pair RDD for joined features
   */
  def spatialJoinPBSM(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate,
                      numMBRTests: LongAccumulator = null): RDD[(IFeature, IFeature)] = {
    // Compute the MBR of the intersection area
    r1.sparkContext.setJobGroup("Analyzing", "Summarizing r1 and r2 for PBSM")
    val mbr1Async = Summary.computeForFeaturesAsync(r1)
    val mbr2Async = Summary.computeForFeaturesAsync(r2)
    val mbr1 = mbr1Async.get()
    val mbr2 = mbr2Async.get()
    val intersectionMBR = mbr1.intersectionEnvelope(mbr2);
    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = mbr1.size + mbr2.size
    val numCells: Int = ((totalSize / (16 * 1024 * 1024)).toInt * 100) max 1
    val gridPartitioner = new GridPartitioner(intersectionMBR, numCells)
    gridPartitioner.setup(new BeastOptions(), true)
    // Co-partition both datasets  using the same partitioner
    val r1Partitioned: RDD[(Int, IFeature)] = r1.partitionBy(gridPartitioner)
    val r2Partitioned: RDD[(Int, IFeature)] = r2.partitionBy(gridPartitioner)
    val joined: RDD[(Int, (Iterable[IFeature], Iterable[IFeature]))] = r1Partitioned.cogroup(r2Partitioned)

    r1.sparkContext.setJobGroup("SpatialJoin", s"Partition based spatial-merge join with ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val partitionID = r._1
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      gridPartitioner.getPartitionMBR(partitionID, dupAvoidanceMBR)
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /**
   * Performs a partition-based spatial-merge (PBSM) join as explained in the following paper.
   * Jignesh M. Patel, David J. DeWitt:
   * Partition Based Spatial-Merge Join. SIGMOD Conference 1996: 259-270
   * https://doi.org/10.1145/233269.233338
   *
   * (Java shortcut)
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @param numMBRTests   (output) the number of MBR tests done during the algorithm
   * @return a pair RDD for joined features
   */
  def spatialJoinPBSM(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: ESJPredicate,
                      numMBRTests: LongAccumulator): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinPBSM(r1.rdd, r2.rdd, joinPredicate, numMBRTests))

  /**
   * Performs a partition-based spatial-merge (PBSM) join as explained in the following paper.
   * Jignesh M. Patel, David J. DeWitt:
   * Partition Based Spatial-Merge Join. SIGMOD Conference 1996: 259-270
   * https://doi.org/10.1145/233269.233338
   *
   * (Java shortcut)
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @return a pair RDD for joined features
   */
  def spatialJoinPBSM(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: ESJPredicate)
  : JavaPairRDD[IFeature, IFeature] = spatialJoinPBSM(r1, r2, joinPredicate, null)

  /**
   * Runs a spatial join between the two given RDDs using the block-nested-loop join algorithm.
   *
   * @param r1            the first set of features
   * @param r2            the second set of features
   * @param joinPredicate the predicate that joins a feature from r1 with a feature in r2
   * @return
   */
  def spatialJoinBNLJ(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate,
                      numMBRTests: LongAccumulator = null): RDD[(IFeature, IFeature)] = {
    // Convert the two RDD to arrays
    val f1: RDD[Array[IFeature]] = r1.glom()
    val f2: RDD[Array[IFeature]] = r2.glom()

    // Combine them using the Cartesian product (as in block nested loop)
    val f1f2 = f1.cartesian(f2)

    r1.sparkContext.setJobGroup("SpatialJoin", s"Block-nested loop join with ${f1f2.getNumPartitions} partitions")

    // For each pair of blocks, run the spatial join algorithm
    f1f2.flatMap(p1p2 => {
      // Extract the two arrays of features
      val p1: Array[IFeature] = p1p2._1
      val p2: Array[IFeature] = p1p2._2

      // Duplicate avoidance MBR is set to infinity (include all space) in the BNLJ algorithm
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      dupAvoidanceMBR.setInfinite()
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /** Java shortcut */
  def spatialJoinBNLJ(r1: JavaSpatialRDD, r2: JavaSpatialRDD,
                      joinPredicate: ESJPredicate, numMBRTests: LongAccumulator): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinBNLJ(r1.rdd, r2.rdd, joinPredicate, numMBRTests))

  /** Java shortcut without MBR count */
  def spatialJoinBNLJ(r1: JavaSpatialRDD, r2: JavaSpatialRDD,
                      joinPredicate: ESJPredicate): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinBNLJ(r1.rdd, r2.rdd, joinPredicate, null))

  /**
   * Distributed join algorithm between spatially partitioned RDDs that ar eloaded from disk
   *
   * @param r1            the first set of features
   * @param r2            the second set of features
   * @param joinPredicate the predicate that joins a feature from r1 with a feature in r2
   * @param numMBRTests   a counter that will contain the number of MBR tests
   * @return a pair RDD for joined features
   */
  def spatialJoinDJIndexedFiles(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate,
                                numMBRTests: LongAccumulator = null): RDD[(IFeature, IFeature)] = {
    require(r1.partitioner.isDefined && r1.partitioner.get.isInstanceOf[SparkSpatialPartitioner],
      "r1 should be spatially partitioned")
    require(r2.partitioner.isDefined && r2.partitioner.get.isInstanceOf[SparkSpatialPartitioner],
      "r2 should be spatially partitioned")
    val matchingPartitions: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      new SpatialIntersectionRDD1(r1, r2)
    r1.sparkContext.setJobGroup("SpatialJoin", s"Distributed join with ${matchingPartitions.getNumPartitions} partitions")
    matchingPartitions.flatMap(joinedPartition => {
      val dupAvoidanceMBR: EnvelopeNDLite = joinedPartition._1
      // Extract the two arrays of features
      val p1: Array[IFeature] = joinedPartition._2._1.toArray
      val p2: Array[IFeature] = joinedPartition._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /**
   * Distributed join algorithm between spatially partitioned RDDs that were partitioned in memory
   *
   * @param r1            the first set of features
   * @param r2            the second set of features
   * @param joinPredicate the predicate that joins a feature from r1 with a feature in r2
   * @param numMBRTests   a counter that will contain the number of MBR tests
   * @return a pair RDD for joined features
   */
  def spatialJoinDJPartitionedRDDs(r1: PartitionedSpatialRDD, r2: PartitionedSpatialRDD, joinPredicate: ESJPredicate,
                                   numMBRTests: LongAccumulator = null): RDD[(IFeature, IFeature)] = {
    require(r1.partitioner.isDefined && r1.partitioner.get.isInstanceOf[SparkSpatialPartitioner],
      "r1 should be spatially partitioned")
    require(r2.partitioner.isDefined && r2.partitioner.get.isInstanceOf[SparkSpatialPartitioner],
      "r2 should be spatially partitioned")
    val matchingPartitions: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      new SpatialIntersectionRDD2(r1, r2)
    r1.sparkContext.setJobGroup("SpatialJoin", s"Distributed join with ${matchingPartitions.getNumPartitions} partitions")
    matchingPartitions.flatMap(joinedPartition => {
      val dupAvoidanceMBR: EnvelopeNDLite = joinedPartition._1
      // Extract the two arrays of features
      val p1: Array[IFeature] = joinedPartition._2._1.toArray
      val p2: Array[IFeature] = joinedPartition._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /***
   * Repartition join algorithm between two datasets: r1 is spatially disjoint partitioned and r2 is not
   * @param r1 the first dataset
   * @param r2 the second dataset
   * @param joinPredicate the join predicate
   * @param numMBRTests an optional accumulator that counts the number of MBR tests
   * @return an RDD of pairs of matching features
   */
  def spatialJoinRepJ(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate,
                      numMBRTests: LongAccumulator = null): RDD[(IFeature, IFeature)] = {
    require(r1.isSpatiallyPartitioned || r2.isSpatiallyPartitioned,
      "Repartition join requires at least one of the two datasets to be spatially partitioned")
    // Choose which dataset to repartition, 1 for r1, and 2 for r2
    // If only one dataset is partitioned, always repartition the other one
    // If both are partitioned, repartition the smaller one
    val whichDatasetToPartition: Int = if (!r1.isSpatiallyPartitioned)
      1
    else if (!r2.isSpatiallyPartitioned)
      2
    else {
      // Choose the smaller dataset
      r1.sparkContext.setJobGroup("Analyzing", "Estimating the size of r1 and r2 for REPJ")
      val mbr1Async = Summary.computeForFeaturesAsync(r1)
      val mbr2Async = Summary.computeForFeaturesAsync(r2)
      val size1: Long = mbr1Async.get().size
      val size2: Long = mbr2Async.get().size
      if (size1 < size2) 1 else 2
    }
    val (r1Partitioned: SpatialRDD, r2Partitioned: SpatialRDD) = if (whichDatasetToPartition == 1) {
      // Repartition r1 according to r2
      val partitioner = new CellPartitioner(r2.partitioner.get.asInstanceOf[SparkSpatialPartitioner].getSpatialPartitioner)

      // Co-partition both datasets using the same partitioner
      val r1Partitioned: RDD[IFeature] = r1.partitionBy(partitioner).mapPartitions(_.map(_._2), preservesPartitioning = true)
      (r1Partitioned, r2)
    } else {
      // Repartition r2 according to r1
      val partitioner = new CellPartitioner(r1.partitioner.get.asInstanceOf[SparkSpatialPartitioner].getSpatialPartitioner)

      // Co-partition both datasets using the same partitioner
      val r2Partitioned: RDD[IFeature] = r2.partitionBy(partitioner).mapPartitions(_.map(_._2), preservesPartitioning = true)
      (r1, r2Partitioned)
    }
    val joined: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      new SpatialIntersectionRDD1(r1Partitioned, r2Partitioned)

    r1.sparkContext.setJobGroup("SpatialJoin", s"Repartition Join with ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val dupAvoidanceMBR: EnvelopeNDLite = r._1
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }
}
