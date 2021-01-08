package edu.ucr.cs.bdlab.beast.cg;

import org.apache.spark.Partitioner;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A wrapper around a {@link SpatialPartitioner} to make it a Spark partitioner.
 * 用于spark环境中的分区，暂时未看到与非spark环境的区别
 */
public class SparkSpatialPartitioner extends Partitioner implements Externalizable {

  /** The internal partitioner */
  protected SpatialPartitioner spatialPartitioner;

  /** A default constructor for deserialization */
  public SparkSpatialPartitioner() {}

  public SparkSpatialPartitioner(SpatialPartitioner spatialPartitioner) {
    this.spatialPartitioner = spatialPartitioner;
  }

  @Override
  public int numPartitions() {
    return spatialPartitioner.getPartitionCount();
  }

  @Override
  public int getPartition(Object key) {
    // They key is an Integer that points to a partition number and we just return it
    // We do this because this method cannot return multiple partitions which is needed for disjoint partitions
    return (Integer) key;
  }

  /**
   * Returns the contained spatial partitioner.
   * @return the spatial partitioner contained in this object
   */
  public SpatialPartitioner getSpatialPartitioner() {
    return spatialPartitioner;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(spatialPartitioner.getClass().getName());
    spatialPartitioner.writeExternal(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    try {
      String partitionerClassName = in.readUTF();
      Class<? extends SpatialPartitioner> partitionerClass = Class.forName(partitionerClassName).asSubclass(SpatialPartitioner.class);

      this.spatialPartitioner = partitionerClass.newInstance();
      this.spatialPartitioner.readExternal(in);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Partitioner class not found", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Cannot access the default constructor", e);
    } catch (InstantiationException e) {
      throw new RuntimeException("Cannot instantiate the spatial partitioner", e);
    }
  }
}
