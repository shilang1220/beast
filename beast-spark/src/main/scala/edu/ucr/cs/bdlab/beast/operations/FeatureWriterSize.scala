package edu.ucr.cs.bdlab.beast.operations

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.io.{FeatureWriter, SpatialFileRDD, SpatialOutputFormat}
import org.apache.hadoop.io.IOUtils.NullOutputStream

/**
 * A size estimator based on a FeatureWriter that cached the FeatureWriter locally to avoid recreating it
 * with every call but also does not serialize it since FeatureWriter is not necessarily serializable
 *
 * @param opts user options to create the feature writer and initialize it
 */
class FeatureWriterSizeFunction(opts: BeastOptions) extends (IFeature => Int)
  with org.apache.spark.api.java.function.Function[IFeature, Int]
  with Serializable {
  @transient var writer: FeatureWriter = _

  require(opts.get(SpatialOutputFormat.OutputFormat).isDefined || opts.get(SpatialFileRDD.InputFormat).isDefined,
    s"The output format must be defined by setting the parameter ${SpatialOutputFormat.OutputFormat}''")

  def getOrCreateWriter: FeatureWriter = {
    if (writer == null) {
      val writerClass = SpatialOutputFormat.getConfiguredFeatureWriterClass(opts.loadIntoHadoopConf(null))
      val featureWriter = writerClass.newInstance
      featureWriter.initialize(new NullOutputStream, opts.loadIntoHadoopConf(null))
      writer = featureWriter
    }
    writer
  }

  override def apply(f: IFeature): Int = getOrCreateWriter.estimateSize(f)

  /**For Java callers*/
  override def call(f: IFeature): Int = apply(f)
}