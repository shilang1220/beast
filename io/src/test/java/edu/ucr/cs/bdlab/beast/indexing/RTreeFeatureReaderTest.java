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

import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RTreeFeatureReaderTest extends JavaSparkTest {
  public static GeometryFactory geometryFactory = new GeometryFactory();

  public void testReadAll() throws IOException {
    IFeature features[] = {
        Feature.create(new PointND(geometryFactory, 1.0, 2.4), null, null, new Object[] {"abc", "def"}),
        Feature.create(new PointND(geometryFactory, 3.0, 4.4), null, null, new Object[] {"abcc", "deff"}),
        Feature.create(new PointND(geometryFactory, 5.0, 6.4), null, null, new Object[] {"abbc", "deef"}),
        Feature.create(new PointND(geometryFactory, 7.0, 8.4), null, null, new Object[] {"aabc", "ddef"}),
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
    FileSystem fs = rtreePath.getFileSystem(conf);
    FileSplit fsplit = new FileSplit(rtreePath, 0, fs.getFileStatus(rtreePath).getLen(), new String[0]);
    try {
      reader.initialize(fsplit, conf);

      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }

      assertEquals(features.length, count);
    } finally {
      reader.close();
    }
  }

  public void testRangeSearch() throws IOException, InterruptedException {
    IFeature features[] = {
        Feature.create(new PointND(geometryFactory, 1.0, 2.4), null, null, new Object[] {"abc", "def"}),
        Feature.create(new PointND(geometryFactory, 3.0, 4.4), null, null, new Object[] {"abcc", "deff"}),
        Feature.create(new PointND(geometryFactory, 5.0, 6.4), null, null, new Object[] {"abbc", "deef"}),
        Feature.create(new PointND(geometryFactory, 7.0, 8.4), null, null, new Object[] {"aabc", "ddef"}),
    };

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    conf.set(SpatialFileRDD.FilterMBR(), "0,0,4,5");
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(null, f);
    }
    writer.close(null);

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    FileSystem fs = rtreePath.getFileSystem(conf);
    FileSplit fsplit = new FileSplit(rtreePath, 0, fs.getFileStatus(rtreePath).getLen(), new String[0]);
    try {
      reader.initialize(fsplit, conf);

      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }

      assertEquals(2, count);
    } finally {
      reader.close();
    }
  }

  public void testRangeSearchContained() throws IOException, InterruptedException {
    IFeature features[] = {
        Feature.create(new PointND(geometryFactory, 1.0, 2.4), null, null, new Object[] {"abc", "def"}),
        Feature.create(new PointND(geometryFactory, 3.0, 4.4), null, null, new Object[] {"abcc", "deff"}),
        Feature.create(new PointND(geometryFactory, 5.0, 6.4), null, null, new Object[] {"abbc", "deef"}),
        Feature.create(new PointND(geometryFactory, 7.0, 8.4), null, null, new Object[] {"aabc", "ddef"}),
    };

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    conf.set(SpatialFileRDD.FilterMBR(), "0,0,8,10");
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(null, f);
    }
    writer.close(null);

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    FileSystem fs = rtreePath.getFileSystem(conf);
    FileSplit fsplit = new FileSplit(rtreePath, 0, fs.getFileStatus(rtreePath).getLen(), new String[0]);
    try {
      reader.initialize(fsplit, conf);

      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }

      assertEquals(4, count);
    } finally {
      reader.close();
    }
  }

  public void testReadWithSchema() throws IOException, InterruptedException {
    IFeature features[] = {
        Feature.create(new PointND(geometryFactory, 1.0, 2.4), null, null, new Object[] {"abc", "def"}),
        Feature.create(new PointND(geometryFactory, 3.0, 4.4), null, null, new Object[] {"abcc", "deff"}),
        Feature.create(new PointND(geometryFactory, 5.0, 6.4), null, null, new Object[] {"abbc", "deef"}),
        Feature.create(new PointND(geometryFactory, 7.0, 8.4), null, null, new Object[] {"aabc", "ddef"}),
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
    List<IFeature> features2 = new ArrayList<>();
    RTreeFeatureReader reader = new RTreeFeatureReader();
    FileSystem fs = rtreePath.getFileSystem(conf);
    FileSplit fsplit = new FileSplit(rtreePath, 0, fs.getFileStatus(rtreePath).getLen(), new String[0]);
    try {
      reader.initialize(fsplit, conf);

      int count = 0;
      while (reader.nextKeyValue()) {
        IFeature feature = reader.getCurrentValue();
        features2.add(feature);
        assertEquals(2, feature.getNumAttributes());
        count++;
      }

      assertEquals(features.length, count);
    } finally {
      reader.close();
    }
  }
}