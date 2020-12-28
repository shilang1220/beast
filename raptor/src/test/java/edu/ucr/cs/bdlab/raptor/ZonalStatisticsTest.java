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
package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.shapefile.ShapefileGeometryReader;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.locationtech.jts.geom.Geometry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZonalStatisticsTest extends JavaSparkTest {

  static final double[] expectedCounts = {17,25,15,2,14,13,17,27,15,17,25,17,11,20,8,8,12,12,16,14,10,7,39,1,3,1,0,2,9,9,
      10,7,5,9,8,6,7,8,5,8,14,3,0,0,2,0,10,8,6,17,178};
  static final double[] expectedSums = {203,260,210,40,119,113,169,229,133,180,228,164,126,201,90,128,164,168,228,147,
      124,40,461,2,24,6,0,4,30,105,95,57,58,144,132,54,82,118,60,32,96,10,0,0,4,0,66,58,28,246,2326};

  public void testEachPolygonSeparatelyAllAlgorithms() throws IOException {
    // Read the geometries from the shapefile
    Path shapefile = new Path(scratchPath(), "shapefile.zip");
    copyResource("/vectors/ne_110m_admin_1_states_provinces.zip", new File(shapefile.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    Geometry[] geometries;
    Configuration conf = new Configuration();
    try  {
      reader.initialize(shapefile, conf);
      List<Geometry> geometryList = new ArrayList<>();
      while (reader.nextKeyValue()) {
        geometryList.add(reader.getCurrentValue());
      }
      geometries = geometryList.toArray(new Geometry[geometryList.size()]);
      assertEquals(expectedCounts.length, geometries.length);
    } finally {
      reader.close();
    }

    // Open the GeoTIFF file
    Path geoTiffFile = new Path(scratchPath(), "test.tif");
    copyResource("/rasters/glc2000_small.tif", new File(geoTiffFile.toString()));
    GeoTiffReader raster = new GeoTiffReader();
    FileSystem fileSystem = geoTiffFile.getFileSystem(conf);

    try {
      raster.initialize(fileSystem, geoTiffFile, 0);
      for (int $i = 0; $i < geometries.length; $i++) {
        Collector[] results;
        Statistics stats;
        for (int iAlgoritm = 0; iAlgoritm < 3; iAlgoritm++) {
          switch (iAlgoritm) {
            case 0:
              results = ZonalStatisticsCore.computeZonalStatisticsScanline(raster, new Geometry[] {geometries[$i]},
                  Statistics.class);
              break;
            case 1:
              results = ZonalStatisticsCore.computeZonalStatisticsNaive(raster, new Geometry[] {geometries[$i]},
                  Statistics.class);
              break;
            case 2:
              results = ZonalStatisticsCore.computeZonalStatisticsQuadSplit(raster, new Geometry[] {geometries[$i]},
                  Statistics.class);
              break;
            default:
              throw new RuntimeException(String.format("Unknown algorithm %d", iAlgoritm));
          }
          if (results.length == 0) {
            assertEquals(0.0, expectedCounts[$i]);
          } else {
            stats = (Statistics) results[0];
            assertEquals(String.format("Error in method #%d with the count of polygon #%d '%s'", iAlgoritm, $i, geometries[$i].toText()),
                    expectedCounts[$i], stats.count[0]);
            assertEquals(String.format("Error in method #%d with the sum of polygon #%d '%s'", iAlgoritm, $i, geometries[$i].toText()),
                    expectedSums[$i], stats.sum[0]);
          }
        }
      }
    } finally {
      raster.close();
    }
  }

  public void testAllPolygonsTogetherScanline() throws IOException {
    // Read the geometries from the shapefile
    Path shapefile = new Path(scratchPath(), "shapefile.zip");
    copyResource("/vectors/ne_110m_admin_1_states_provinces.zip", new File(shapefile.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    Geometry[] geometries;
    Configuration conf = new Configuration();
    try  {
      reader.initialize(shapefile, conf);
      List<Geometry> geometryList = new ArrayList<>();
      while (reader.nextKeyValue()) {
        geometryList.add(reader.getCurrentValue());
      }
      geometries = geometryList.toArray(new Geometry[geometryList.size()]);
      assertEquals(expectedCounts.length, geometries.length);
    } finally {
      reader.close();
    }

    // Open the GeoTIFF file
    Path geoTiffFile = new Path(scratchPath(), "test.tif");
    copyResource("/rasters/glc2000_small.tif", new File(geoTiffFile.toString()));
    GeoTiffReader raster = new GeoTiffReader();
    FileSystem fileSystem = geoTiffFile.getFileSystem(conf);
    try {
      raster.initialize(fileSystem, geoTiffFile, 0);
      Collector[] results = ZonalStatisticsCore.computeZonalStatisticsScanline(raster, geometries, Statistics.class);
      assertEquals(expectedCounts.length, results.length);

      for (int $i = 0; $i < results.length; $i++) {
        Statistics stats = (Statistics) results[$i];
        if (expectedCounts[$i] == 0.0) {
          assertNull(stats);
        } else {
          assertEquals(String.format("Error with the count of polygon #%d '%s'", $i, geometries[$i].toText()),
              expectedCounts[$i], stats.count[0]);
          assertEquals(String.format("Error with the sum of polygon #%d '%s'", $i, geometries[$i].toText()),
              expectedSums[$i], stats.sum[0]);
        }
      }
    } finally {
      raster.close();
    }
  }

  public void testReprojectVector() throws IOException {
    Map<String, Double> countsByName = new HashMap<>();
    Map<String, Double> sumsByName = new HashMap<>();

    File vectorInput = makeFileCopy("/raptor/ne_110m_admin_1_states_provinces_EPSG3857.zip");
    File rasterInput = makeFileCopy("/raptor/glc2000_small.tif");
    File outputFile = new File(scratchDir(), "out");

    BeastOptions opts = new BeastOptions(false)
        .set("iformat", "shapefile")
        .set("rformat", "geotiff")
        .set("oformat", "wkt")
        .setBoolean(CSVFeatureWriter.WriteHeader, true);

    final IntWritable i = new IntWritable();
    SpatialReader.readInput(javaSparkContext(), opts, vectorInput.getPath(), opts.getString(SpatialFileRDD.InputFormat()))
        .collect()
        .forEach(feature -> {
          String name = feature.getAs("name");
          countsByName.put(name, expectedCounts[i.get()]);
          sumsByName.put(name, expectedSums[i.get()]);
          i.set(i.get()+1);
        });

    ZonalStatistics.setup(opts);
    ZonalStatistics.run(opts, new String[] {vectorInput.getPath(), rasterInput.getPath()},
        new String[] {outputFile.getPath()}, sparkContext());

    assertFileExists(outputFile.getPath());
    // Read the output and verify
    opts = new BeastOptions(false).set("iformat", "wkt").setBoolean("skipheader", true);
    CSVFeatureReader reader = new CSVFeatureReader();
    try {
      reader.initialize(new Path(outputFile.getPath(), "part-r-00000.csv"), opts.loadIntoHadoopConf(new Configuration()));
      int count = 0;
      while (reader.nextKeyValue()) {
        IFeature feature = reader.getCurrentValue();
        while (count < expectedCounts.length && expectedCounts[count] == 0.0)
          count++;
        double expectedSum = sumsByName.get(feature.getAs("name"));
        double expectedCount = countsByName.get(feature.getAs("name"));
        assertEquals("Error in count with "+feature, expectedCount, Double.parseDouble(feature.getAs("count")));
        assertEquals("Error in sum with "+feature, expectedSum, Double.parseDouble(feature.getAs("sum")));
        count++;
      }
      assertEquals(expectedCounts.length, count);
    } finally {
      reader.close();
    }
  }

  public void testProcessHDFDirectory() throws IOException {
    File vectorInput = new File(scratchDir(), "in.zip");
    File rasterDir = new File(scratchDir(), "raster");
    rasterDir.mkdirs();
    File rasterInput = new File(rasterDir, "in.hdf");
    File outputFile = new File(scratchDir(), "out");
    copyResource("/raptor/ne_110m_admin_1_states_provinces_EPSG3857.zip", vectorInput);
    copyResource("/raptor/MYD11A1.A2002185.h09v06.006.2015146150958.hdf", rasterInput);

    BeastOptions opts = new BeastOptions(false)
        .set("iformat", "shapefile")
        .set("rformat", "hdf")
        .set("layer", "LST_Day_1km")
        .set("oformat", "wkt")
        .setBoolean(CSVFeatureWriter.WriteHeader, true);
    ZonalStatistics.run(opts, new String[] {vectorInput.getPath(), rasterInput.getPath()},
        new String[] {outputFile.getPath()}, sparkContext());

    assertFileExists(outputFile.getPath());
    // Read the output and verify
    opts = new BeastOptions(false).set("iformat", "wkt").setBoolean("skipheader", true);
    CSVFeatureReader reader = new CSVFeatureReader();
    try {
      reader.initialize(new Path(outputFile.getPath(), "part-r-00000.csv"), opts.loadIntoHadoopConf(null));
      int count = 0;
      while (reader.nextKeyValue())
        count++;
      // For this specific file, only Texas and Louisiana should overlap
      assertEquals(2, count);
    } finally {
      reader.close();
    }
  }
}