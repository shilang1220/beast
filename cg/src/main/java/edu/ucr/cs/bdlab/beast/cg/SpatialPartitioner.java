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
package edu.ucr.cs.bdlab.beast.cg;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Externalizable;
import java.io.PrintStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Iterator;

/**
 * An interface for spatially partitioning data into partitions.
 * @author Ahmed Eldawy
 *
 */
public interface SpatialPartitioner extends Externalizable, Iterable<EnvelopeNDLite> {
  Log LOG = LogFactory.getLog(SpatialPartitioner.class);

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Metadata {
    /**
     * Whether this global indexer supports disjoint partitions or not
     * @return {@code true} if disjoint partitioning is supported for this partitioner
     */
    boolean disjointSupported();

    /**
     * The extension used with the global index. Also used for choosing this partitioner from command line.
     * @return the extension including the dot
     */
    String extension();

    /**
     * A short description for this partitioner
     * @return the description of this partitioner
     */
    String description();
  }

  /**
   * Annotates the required parameters of the {@link #construct(Summary, double[][], AbstractHistogram, int)}
   * function in implementation of this interface.
   */
  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Required {}

  /**
   * Annotates the parameters of the {@link #construct(Summary, double[][], AbstractHistogram, int)}
   * function that can be used but are not required.
   */
  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Preferred {}

  /**
   * Initializes the partitioner before it is first constructed.
   * @param conf the environment configuration
   * @param disjoint set to true to produce disjoint partitions with probable replication of some features
   */
  default void setup(BeastOptions conf, boolean disjoint) {}


  /**
   * Constructs the histogram using the given parameters. By default, only the summary parameter is provided
   * and the other two parameters {@code sample} and {@code histogram} are {@code null}. If any of these are needed,
   * the parameter should be annotated with {@link Required}. If any of them can be used to improve the results but
   * are not necessarily required, they can be annotated with {@link Preferred}.
   * @param summary the summary of the input
   * @param sample a sample points from the input
   * @param histogram a histogram of the input
   * @param numPartitions the desired number of partitions. This is treated as a loose hint and not a strict value
   */
  void construct(Summary summary, double[][] sample, AbstractHistogram histogram, int numPartitions);

  /**
   * Find all the overlapping partitions in an envelope (MBR).
   * @param mbr the MBR to find overlapping partitions
   * @param matchedPartitions an array to use for matched partitions
   */
  void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions);

  default void overlapPartitions(EnvelopeND mbr, IntArray matchedPartitions) {
    overlapPartitions(new EnvelopeNDLite(mbr), matchedPartitions);
  }
  
  /**
   * Returns the single partition that contains the center of the given envelope.
   * This method should return a value in the range [0, {@link #getPartitionCount()}[
   *
   * @param mbr the minimum bounding rectangle to find a partition
   * @return the ID of the best partition that the given MBR should be assigned to.
   */
  default int overlapPartition(EnvelopeND mbr) {
    return overlapPartition(new EnvelopeNDLite(mbr));
  }

  int overlapPartition(EnvelopeNDLite mbr);
  
  /**
   * Returns the minimum bounding rectangle (MBR) of the given partition.
   * @param partitionID the ID of the partition to get its MBR.
   * @param mbr output parameter for the MBR to avoid allocation of objects
   */
  void getPartitionMBR(int partitionID, EnvelopeNDLite mbr);

  default EnvelopeNDLite getPartitionMBR(int partitionID) {
    EnvelopeNDLite mbr = new EnvelopeNDLite(this.getCoordinateDimension());
    getPartitionMBR(partitionID, mbr);
    return mbr;
  }
  
  /**
   * Returns total number of partitions
   * @return the total number of partitions
   */
  int getPartitionCount();

  /**
   * Whether this partitioner is configured to produce disjoint partitions
   * @return {@code true} if this partitions will produce spatially disjoint partitions
   */
  boolean isDisjoint();

  /**
   * Number of dimensions for this partitioner
   * @return the dimensions of the partitioner
   */
  int getCoordinateDimension();

  default Metadata getMetadata() {
    return this.getClass().getAnnotation(Metadata.class);
  }

  /**
   * Returns the MBR of the underlying dataset on which the partitioner was created.
   * @return the envelope of the entire partitioner
   */
  EnvelopeNDLite getEnvelope();

  @Override
  default Iterator<EnvelopeNDLite> iterator() {
    return new EnvelopeIterator(this);
  }

  /**
   * Print all the partition boundaries to a tab-separated file.
   * @param out the output to write to
   */
  default void toWKT(PrintStream out) {
    out.println("PartitionID\tMBR");
    EnvelopeNDLite mbr = new EnvelopeNDLite(this.getCoordinateDimension());
    for (int i = 0; i < getPartitionCount(); i++) {
      out.print(i);
      out.print('\t');
      this.getPartitionMBR(i, mbr);
      out.println(mbr.toWKT(new StringBuilder()).toString());
    }
  }

  /**
   * A class to iterate over all partitions
   */
  class EnvelopeIterator implements Iterator<EnvelopeNDLite> {
    /**The spatial partitioner that this iterate goes over*/
    private final SpatialPartitioner spatialPartitioner;

    /**The partitioner that will be returned when next is called*/
    private int iPartition = 0;

    /**The partition that is returned when next is called*/
    private final EnvelopeNDLite partitionMBR = new EnvelopeNDLite();

    public EnvelopeIterator(SpatialPartitioner p) {
      this.spatialPartitioner = p;
    }

    @Override
    public boolean hasNext() {
      return iPartition < spatialPartitioner.getPartitionCount();
    }

    @Override
    public EnvelopeNDLite next() {
      spatialPartitioner.getPartitionMBR(iPartition, partitionMBR);
      iPartition++;
      return partitionMBR;
    }
  }
}
