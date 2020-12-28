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
package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialOutputFormat;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

public class MultilevelPlotTest extends JavaSparkTest {

  public void testCreateImageTiles() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/viztest.point", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .setInt("levels", 3)
        .setInt("threshold", 0)
        .setBoolean("vflip", false);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    opts.set(SpatialOutputFormat.OutputFormat, "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.png",
        "tile-1-1-1.png",
        "tile-2-0-0.png",
        "tile-2-1-0.png",
        "tile-2-3-1.png",
        "tile-2-2-3.png",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);
    FileSystem fs = FileSystem.getLocal(sparkContext().hadoopConfiguration());
    BeastOptions opts2 = new BeastOptions().loadFromTextFile(fs, new Path(new Path(outputFile.getPath()),
        "_visualization.properties"));
    assertEquals(3, opts2.getInt("levels", 0));
  }

  public void testPlotGeometries() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/viztest.point", inputFile);
    BeastOptions opts = new BeastOptions(false);
    opts.setBoolean(CommonVisualizationHelper.VerticalFlip, false);
    opts.set(CSVFeatureReader.FieldSeparator, ",");
    JavaRDD<IFeature> features = SpatialReader.readInput(javaSparkContext(), opts,
        inputFile.toString(), "point");
    JavaRDD<Geometry> geoms = features.map(f -> f.getGeometry());
    opts.setInt(MultilevelPlot.DataTileThreshold(), 0);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    MultilevelPlot.plotGeometries(geoms, 0, 2, outputFile.toString(), opts);

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.png",
        "tile-1-1-1.png",
        "tile-2-0-0.png",
        "tile-2-1-0.png",
        "tile-2-3-1.png",
        "tile-2-2-3.png",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);
  }

  public void testCreateImageTilesWithPartialHistogram() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/viztest.point", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .setInt("levels", 3)
        .setInt("threshold", 0)
        .setBoolean("vflip", false);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    opts.setLong(MultilevelPlot.MaximumHistogramSize(), 4 * 8);
    opts.set(SpatialOutputFormat.OutputFormat, "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.png",
        "tile-1-1-1.png",
        "tile-2-0-0.png",
        "tile-2-1-0.png",
        "tile-2-3-1.png",
        "tile-2-2-3.png",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);
  }

  public void testCreateImageTilesWithHigherGranularity() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/viztest.point", inputFile);

    // k=25 means that tiles with three or more records will be generated using flat partitioning
    // Tiles with two or fewer records will be generated using pyramid partitioning.
    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .setInt("levels", 3)
        .setInt("threshold", 0)
        .setBoolean("vflip", false)
        .setInt("k", 25)
        .setInt("pointsize", 0);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 2);
    opts.set(SpatialOutputFormat.OutputFormat, "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.png",
        "tile-1-1-1.png",
        "tile-2-0-0.png",
        "tile-2-1-0.png",
        "tile-2-2-3.png",
        "tile-2-3-1.png",
    };
    for (String expectedFile : expectedFiles) {
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());
      BufferedImage expectedImage = ImageIO.read(new File(outputFile, expectedFile));
      InputStream in = this.getClass().getResourceAsStream("/viztest_mplot/"+expectedFile);
      BufferedImage actualImage = ImageIO.read(in);
      in.close();
      assertImageEquals(expectedImage, actualImage);
    }

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);
  }

  public void testMercatorProjection() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/test-mercator.points", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .setInt("levels", 3)
        .setInt("threshold", 0)
        .setBoolean("vflip", false)
        .setBoolean("keep-ratio", true)
        .setBoolean("mercator", true);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    opts.set(SpatialOutputFormat.OutputFormat, "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(21, numFiles);
  }

  public void testMercatorProjectionShouldDisableVFlipByDefault() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/test-mercator2.points", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .setInt("levels", 3)
        .setInt("threshold", 0)
        .setBoolean("mercator", true)
        .setBoolean("data-tiles", false);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    opts.set(SpatialOutputFormat.OutputFormat, "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.png",
        "tile-2-0-0.png",
        "tile-2-1-0.png",
        "tile-2-2-0.png",
        "tile-2-3-0.png",
        "tile-2-0-1.png",
        "tile-2-1-1.png",
        "tile-2-2-1.png",
        "tile-2-3-1.png",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);

    FileSystem fs = new Path(outputFile.getPath()).getFileSystem(opts.loadIntoHadoopConf(new Configuration()));
    BeastOptions opts2 = new BeastOptions().loadFromTextFile(fs, new Path(new Path(outputFile.getPath()),
        "_visualization.properties"));
    assertTrue("Mercator should be set", opts2.getBoolean(CommonVisualizationHelper.UseMercatorProjection, false));
  }

  public void testMercatorProjectionShouldHandleOutOfBoundsObjects() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/test-mercator3.points", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .setInt("levels", 3)
        .setInt("threshold", 0)
        .setBoolean("mercator", true)
        .setInt("pointsize",  0);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    opts.set(SpatialOutputFormat.OutputFormat, "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(3, numFiles);
  }

  public void testKeepRatio() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.rect");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/viztest.rect", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "envelopek(2)")
        .set("separator", ",")
        .setInt("levels", 3)
        .setInt("threshold", 0)
        .setBoolean("vflip", false)
        .setBoolean("keep-ratio", true)
        .setInt("pointsize", 0);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    opts.set(SpatialOutputFormat.OutputFormat, "envelopek(2)");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-1.png",
        "tile-2-0-1.png",
        "tile-2-3-2.png",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);
  }

  public void testCreateImageAndDataTiles() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/viztest.point", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .setInt("levels", 3)
        .setBoolean("vflip", false);
    opts.setInt(MultilevelPlot.DataTileThreshold(), 8 * 2 + GeometryHelper.FixedGeometryOverhead);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    opts.set(SpatialOutputFormat.OutputFormat, "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.csv",
        "tile-1-1-1.csv",
        "tile-2-0-0.csv",
        "tile-2-1-0.csv",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);

    // Make sure that no additional 'part-' files are in the output
    int numPartFiles = outputFile.listFiles(n -> n.getName().startsWith("part-")).length;
    assertEquals(0, numPartFiles);
  }

  public void testCreateDataTilesWithDifferentInputOutputFormats() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/viztest.point", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .set("oformat", "wkt(0)")
        .setInt("levels", 3)
        .setBoolean("vflip", false);
    // Set the threshold equal to one point
    opts.setInt(MultilevelPlot.DataTileThreshold(), 8 * 2 + GeometryHelper.FixedGeometryOverhead);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    opts.set(SpatialOutputFormat.OutputFormat, "wkt(0)");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.csv",
        "tile-1-1-1.csv",
        "tile-2-0-0.csv",
        "tile-2-1-0.csv",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    Path configFile = new Path(outputFile.toString(), "_visualization.properties");
    opts = new BeastOptions(false);
    opts.loadFromTextFile(configFile.getFileSystem(opts.loadIntoHadoopConf(new Configuration())), configFile);
    assertEquals("wkt(0)", opts.get(SpatialFileRDD.InputFormat()).get());
    assertEquals(",", opts.get(CSVFeatureReader.FieldSeparator).get());
  }

  public void testSkipDataTilesAndReuseIndex() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/viztest.point", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .setInt("levels", 3)
        .setBoolean("vflip", false)
        .setBoolean("data-tiles", false);
    opts.setInt(MultilevelPlot.DataTileThreshold(), 8 * 2 + GeometryHelper.FixedGeometryOverhead);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    opts.set(SpatialOutputFormat.OutputFormat, "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    // Make sure that there are no additional tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);

    // Read the configuration file
    Path confFile = new Path(outputFile.getPath(), "_visualization.properties");
    opts = new BeastOptions(false);
    opts.loadFromTextFile(confFile.getFileSystem(new Configuration()), confFile);
    assertEquals("../in.point", opts.get("data").get());
  }

  public void testSkipDataTilesAndReuseIndexWithPlotterClass() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");
    copyResource("/viztest.point", inputFile);
    BeastOptions opts = new BeastOptions(false);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);

    // Read the features in the input dataset
    opts.set(CSVFeatureReader.FieldSeparator, ",");
    JavaRDD<IFeature> input = SpatialReader.readInput(javaSparkContext(), opts,
        inputFile.getPath(), "point");

    opts.setBoolean(MultilevelPlot.IncludeDataTiles(), false);
    opts.setInt(MultilevelPlot.DataTileThreshold(), 8 * 2 + GeometryHelper.FixedGeometryOverhead);
    opts.setBoolean(CommonVisualizationHelper.VerticalFlip, false);
    MultilevelPlot.plotFeatures(input, 0, 2, GeometricPlotter.class, inputFile.toString(),
            outputFile.toString(), opts);

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    // Make sure that there are no additional tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);

    // Read the configuration file
    Path confFile = new Path(outputFile.getPath(), "_visualization.properties");
    opts = new BeastOptions(false);
    opts.loadFromTextFile(confFile.getFileSystem(new Configuration()), confFile);
    assertEquals("../in.point", opts.get("data").get());
    assertEquals(GeometricPlotter.class, opts.getClass(CommonVisualizationHelper.PlotterClassName, null, Plotter.class));
    assertEquals("3", opts.get(MultilevelPlot.NumLevels()).get());
  }


  public void testCreateImageTilesWithBuffer() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out");

    PrintWriter infile = new PrintWriter(new FileOutputStream(inputFile));

    try {
      infile.println("0,0");
      infile.println("2,2");
      infile.println("4,4");
    } finally {
      infile.close();
    }

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .setInt("levels", 3)
        .setInt("threshold", 0)
        .setBoolean("vflip", false)
        .setInt("pointsize", 5);
    opts.setInt(MultilevelPlot.PartitionGranularity(), 1);
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-0-1.png",
        "tile-1-1-0.png",
        "tile-1-1-1.png",
        "tile-2-0-0.png",
        "tile-2-1-1.png",
        "tile-2-1-2.png",
        "tile-2-2-1.png",
        "tile-2-2-2.png",
        "tile-2-3-3.png",
    };
    for (String expectedFile : expectedFiles)
      assertTrue(String.format("Expected tile '%s' not found", expectedFile), new File(outputFile, expectedFile).exists());

    // Make sure that there are no extra tiles were created
    int numFiles = outputFile.listFiles(n -> n.getName().matches("tile-\\d+-\\d+-\\d+.*")).length;
    assertEquals(expectedFiles.length, numFiles);
  }
}
