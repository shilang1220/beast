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
package edu.ucr.cs.bdlab.beast.synopses;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import org.apache.hadoop.io.Writable;

import java.io.Serializable;

/**
 * An abstract class for a histogram that can retrieve the total value for a range.
 */
public abstract class AbstractHistogram extends EnvelopeNDLite implements Serializable {

  abstract public long getValue(int[] minPos, int[] sizes);

  /**
   * Get the number of partitions along the given dimension
   * @param d the number of partitions along the given dimension
   * @return the number of partitions along the given dimension
   */
  abstract public int getNumPartitions(int d);

  /**
   * Returns the total number of bins in the histogram
   * @return the total number of bing in the histogram
   */
  public int getNumBins() {
    int numBins = 1;
    for (int d = 0; d < getCoordinateDimension(); d++) {
      numBins *= getNumPartitions(d);
    }
    return numBins;
  }

  /**
   * Returns a unique ID for the bin (cell) that contains the given point
   * @param coord the coordinate of the point
   * @return the ID of the bin that contains the given point
   */
  public int getBinID(double[] coord) {
    assert coord.length == getCoordinateDimension();
    int[] position = new int[coord.length];
    for (int d = 0; d < coord.length; d++) {
      position[d] = (int) Math.floor((coord[d] - this.getMinCoord(d)) * this.getNumPartitions(d) / this.getSideLength(d));
      position[d] = Math.min(position[d], getNumPartitions(d) - 1);
    }
    int d = position.length;
    int pos = 0;
    while (d-- > 0) {
      pos *= getNumPartitions(d);
      pos += position[d];
    }
    return pos;
  }

  /**
   * Returns the value of the given binID
   * @param binID the ID of the bin
   * @return the value of the given bin
   */
  public abstract long getBinValue(int binID);
}
