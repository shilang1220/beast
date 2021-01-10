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

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.io.{FeatureWriter, SpatialFileRDD, SpatialOutputFormat}
import edu.ucr.cs.bdlab.beast.synopses.{Summary, SummaryAccumulator}
import edu.ucr.cs.bdlab.beast.util.OperationMetadata
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import java.io.ByteArrayOutputStream

/**
  * Computes the summary of the input features including some geometric attributes.
  * beast的一个命令，用于计算数据集的汇总信息，主要包括：整个数据集的最小外包矩形、要素数量、数据集容量大小
 *  - *size*: The estimated size of all features in bytes
 *  - *numFeatures*: Total number of features, i.e., records.
 *  - *numPoints*: The sum of number of points for all geometries in features.
 *  - *numNonEmptyGeometries*: Number of features with non-empty geometries
 *  - *sumSideLength*: The sum of all size lengths of all non-empty geometries. Can be combined with
 *    `numNonEmptyGeometries` to compute the average size per record.
 *  - *geometryType*: The geometry type of all non-empty geometries. This is the least inclusive type of all
 *    geometries. For example, if all geometries are points, the geometry type will be point. If it contains a mix
 *    of points and multi-points, a multipoint type is returned. If it contains a mix of points and polygons,
 *    a GeometryCollection type is used.
  */
@OperationMetadata(
  shortName =  "summary",
  description = "Computes the minimum bounding rectangle (MBR), count, and size of a dataset",
  inputArity = "1",
  outputArity = "0",
  inheritParams = Array(classOf[SpatialFileRDD])
)
object GeometricSummary extends CLIOperation with Logging {

  /** Java shortcut */
  def computeForFeaturesWithOutputSize(features: JavaSpatialRDD, opts: BeastOptions) : Summary =
    computeForFeaturesWithOutputSize(features.rdd, opts)


  /**
    * Compute the summary of the given features while computing the size based on the write size of the configured
    * [[FeatureWriter]] (or output format) in the given user options
   *  计算给定数据集的汇总信息，并基于用户设置的输出格式计算出写的大小
    * @param features features to compute the summary for
    * @param opts the user options that contains the configured output format
    * @return
    */
  def computeForFeaturesWithOutputSize(features : SpatialRDD, opts : BeastOptions) : Summary =
    edu.ucr.cs.bdlab.beast.synopses.Summary.computeForFeatures(features, new FeatureWriterSizeFunction(opts))

  /**
    * Create a summary accumulator that uses the configured output format to measure the size of the output.
   *  使用用户配置的输出格式，测量输出尺寸，创建汇总器
    *
    * @param sc the spark context to register the accumulator to
    * @param opts user options that contain information about which FeatureWriter (or output format) to use
    * @return the initialized and registered accumulator
    */
  def createSummaryAccumulatorWithWriteSize(sc: SparkContext, opts : BeastOptions) : SummaryAccumulator = {
    //Summary为伴生对象
    Summary.createSummaryAccumulator(sc, new FeatureWriterSizeFunction(opts))
  }

  /**
    * Run the main function using the given user command-line options and spark context
    *
    * @param opts user options and configuration
    * @param sc the spark context to use
    * @return the summary computed for the input defined in the user options
    */
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val inputFeatures = sc.spatialFile(inputs(0), opts)

    // 计算汇总信息
    val summary =
    try {
      // If we can get some feature writer class successfully, compute a summary with output format
      // 根据用户参数创建写格式器
      val featureWriter = SpatialOutputFormat.getConfiguredFeatureWriterClass(opts.loadIntoHadoopConf(null))
      // 为没有指定输出大小的数据集输出容量大小
      computeForFeaturesWithOutputSize(inputFeatures, opts)
    } catch {
      case e: Exception => Summary.computeForFeatures(inputFeatures)
    }

    val feature: IFeature = inputFeatures.first()

    // Writing directly to System.out caused the tests to terminate incorrectly in IntelliJ IDEA
    // 输出为JSON格式
    val generatedJSON = new ByteArrayOutputStream()
    val jsonGenerator: JsonGenerator = new JsonFactory().createGenerator(generatedJSON)
    jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter)

    //Summary为伴生对象
    Summary.writeSummaryWithSchema(jsonGenerator, summary, feature)
    jsonGenerator.close
    System.out.write(generatedJSON.toByteArray)
    System.out.println
    summary
  }
}
