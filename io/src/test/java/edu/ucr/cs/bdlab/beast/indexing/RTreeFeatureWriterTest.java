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
package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import edu.ucr.cs.bdlab.beast.util.CounterOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import java.io.IOException;
import java.util.Random;


public class RTreeFeatureWriterTest extends JavaSparkTest {

  public static GeometryFactory geometryFactory = new GeometryFactory();

  public void testWrite() throws IOException, InterruptedException {
    IFeature features[] = {
        new Feature(new PointND(geometryFactory, 1.0, 2.4), null, null, new String[] {"abc", "def"}),
        new Feature(new PointND(geometryFactory, 3.0, 4.4), null, null, new String[] {"abcc", "deff"}),
        new Feature(new PointND(geometryFactory, 5.0, 6.4), null, null, new String[] {"abbc", "deef"}),
        new Feature(new PointND(geometryFactory, 7.0, 8.4), null, null, new String[] {"aabc", "ddef"}),
    };

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(null, f);
    }
    writer.close(null);

    // Now, read the data back
    FileSystem fs = rtreePath.getFileSystem(conf);
    long fileLength = fs.getFileStatus(rtreePath).getLen();
    FSDataInputStream in = fs.open(rtreePath);
    RTreeFeatureReader.readFeatureHeader(in);

    String crsWKT = in.readUTF();
    int rtreeSize = (int) (fileLength - in.getPos());

    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.readFields(in, rtreeSize, input -> null);
    Iterable<RTreeGuttman.Entry> results = rtree.search(new EnvelopeNDLite(2, 0, 0, 4, 5));
    int resultCount = 0;
    for (Object o : results)
      resultCount++;
    assertEquals(2, resultCount);
    rtree.close();
  }

  public void testWriteWithEmptyGeometries() throws IOException {
    IFeature features[] = {
        new Feature(new PointND(geometryFactory, 1.0, 2.4), null, null, new String[] {"abc", "def"}),
        new Feature(new PointND(geometryFactory, 3.0, 4.4), null, null, new String[] {"abcc", "deff"}),
        new Feature(new PointND(geometryFactory, 5.0, 6.4), null, null, new String[] {"abbc", "deef"}),
        new Feature(new PointND(geometryFactory, 7.0, 8.4), null, null, new String[] {"aabc", "ddef"}),
    };

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(null, f);
    }
    writer.close(null);

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    reader.initialize(new FileSplit(rtreePath, 0, rtreePath.getFileSystem(conf).getFileStatus(rtreePath).getLen(), null), conf);
    int count = 0;
    for (Object o : reader) {
      count++;
    }
    assertEquals(4, count);
    reader.close();
  }

  public void testEstimateSize() throws IOException {
    IFeature features[] = {
        new Feature(new PointND(geometryFactory, 1.0, 2.4), null, null, new String[] {"abc", "def"}),
        new Feature(new PointND(geometryFactory, 3.0, 4.4), null, null, new String[] {"abcc", "deff"}),
        new Feature(new PointND(geometryFactory, 5.0, 6.4), null, null, new String[] {"abbc", "deef"}),
        new Feature(new PointND(geometryFactory, 7.0, 8.4), null, null, new String[] {"aabc", "ddef"}),
    };

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Configuration conf = new Configuration();
    writer.initialize(new CounterOutputStream(), conf);
    long size = 0;
    for (IFeature f : features) {
      size += writer.estimateSize(f);
    }
    writer.close(null);
    // No actual assert is needed. Just make sure that it did not fail
    assertTrue(size > 0);
  }

  public void testWriteWithCRS() throws IOException, InterruptedException {
    geometryFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 3857);
    IFeature features[] = {
        new Feature(new PointND(geometryFactory, 1.0, 2.4), null, null, new String[] {"abc", "def"}),
        new Feature(new PointND(geometryFactory, 3.0, 4.4), null, null, new String[] {"abcc", "deff"}),
        new Feature(new PointND(geometryFactory, 5.0, 6.4), null, null, new String[] {"abbc", "deef"}),
        new Feature(new PointND(geometryFactory, 7.0, 8.4), null, null, new String[] {"aabc", "ddef"}),
    };

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(null, f);
    }
    writer.close(null);

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    reader.initialize(new FileSplit(rtreePath, 0, rtreePath.getFileSystem(conf).getFileStatus(rtreePath).getLen(), null), conf);
    assertTrue(reader.nextKeyValue());
    assertEquals(3857, reader.getCurrentValue().getGeometry().getSRID());
    reader.close();
  }

  public void testWriteMultipleRTrees() throws IOException {
    // Writing too many data should create multiple internal R-trees
    int numRecords = 10000;
    Random rand = new Random(0);
    IFeature features[] = new IFeature[numRecords];
    for (int i = 0; i < numRecords; i++)
      features[i] = new Feature(new PointND(geometryFactory, rand.nextDouble(), rand.nextDouble()));

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    // Use a small number to ensure that multiple R-trees will be written
    conf.setInt(RTreeFeatureWriter.MaxSizePerRTree, 1000);
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(null, f);
    }
    writer.close(null);

    // Now, read the data back
    FileSystem fs = rtreePath.getFileSystem(conf);
    long fileLength = fs.getFileStatus(rtreePath).getLen();
    try (FSDataInputStream in = fs.open(rtreePath)) {
      RTreeFeatureReader.readFeatureHeader(in);
      String crsWKT = in.readUTF();
      int numRTrees = 0;
      long dataStart = in.getPos();
      long treeEnd = fileLength;
      while (treeEnd > dataStart) {
        in.seek(treeEnd - 4);
        int treeSize = in.readInt();
        treeEnd -= treeSize + 4;
        numRTrees++;
      }
      assertEquals(dataStart, treeEnd);
      assertTrue("File should contain more than one R-tree", numRTrees > 1);
    }
    // Read the file back using RTreeFeatureReader
    try (RTreeFeatureReader reader = new RTreeFeatureReader()) {
      reader.initialize(new FileSplit(rtreePath, 0, fileLength, null), conf);
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        recordCount++;
      }
      assertEquals(numRecords, recordCount);
    }
  }
}