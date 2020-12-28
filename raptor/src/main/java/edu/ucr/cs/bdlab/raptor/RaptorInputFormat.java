/*
 * Copyright 2019 University of California, Riverside
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

import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An input format that reads a pair of raster and vector files.
 * The raster input consists of a set of raster files and the vector input consists
 * of a set of intersection files.
 * It generates a list of RaptorSplits where each split is defined by a raster file and
 * a set of tile IDs in that raster file as well as the corresponding intersection file.
 */
public class RaptorInputFormat extends InputFormat {

  /**Path to the input raster*/
  public static final String RasterInputPath = "RaptorInputFormat.RasterInputPath";

  /**Path to the input vector (intersection files)*/
  public static final String VectorInputPath = "RaptorInputFormat.VectorInputPath";

  /**
   * Set the path to the raster input.
   * @param conf the configuration used to initialize the file system and read the file
   * @param rasterPath the path to the raster file
   */
  public static void setRasterInputPath(JobConf conf, Path rasterPath) {
    conf.set(RasterInputPath, rasterPath.toString());
  }

  /**
   * Set the path to the vector input (intersection files).
   * @param conf the configuration used to initialize the file system and read the file
   * @param vectorPath the path to the intersection file
   */
  public static void setVectorInputPath(JobConf conf, Path vectorPath) {
    conf.set(VectorInputPath, vectorPath.toString());
  }

  public static Path[] listFiles(FileSystem fileSystem, Path path) throws IOException {
    if (fileSystem.getFileStatus(path).isFile())
      return new Path[]{path};
    FileStatus[] statuses = fileSystem.listStatus(path, SpatialFileRDD.HiddenFileFilter());
    Path[] paths = new Path[statuses.length];
    for (int i = 0; i < statuses.length; i++)
      paths[i] = statuses[i].getPath();
    return paths;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    // List all the raster files to consider
    Path rasterPath = new Path(conf.get(RasterInputPath));
    try (FileSystem rasterFS = rasterPath.getFileSystem(conf)) {
      Path vectorPath = new Path(conf.get(VectorInputPath));
      Path[] rasters = listFiles(rasterFS, rasterPath);

      List<InputSplit> splits = new ArrayList<>();
      // Create a split for each (roughly) 128MB worth of pixels
      for (Path rasterFile : rasters) {
        try (RasterReader reader = RasterHelper.createRasterReader(rasterFS, rasterFile, conf)) {
          long totalNumberOfPixels = (long) reader.getRasterWidth() * reader.getRasterHeight();
          int tilesPerSplit = (int) (totalNumberOfPixels / 128 / 1024 / 1024);
          int totalNumOfTiles = reader.getNumTiles();
          long pixelsPerTile = totalNumberOfPixels / totalNumOfTiles;
          int numSplits = (totalNumOfTiles + tilesPerSplit - 1) / tilesPerSplit;
          tilesPerSplit = (totalNumOfTiles + numSplits - 1) / numSplits;
          int tileStart = 0;
          while (tileStart < totalNumOfTiles) {
            int tileEnd = Math.max(totalNumOfTiles - 1, tileStart + tilesPerSplit - 1);
            RaptorSplit split = new RaptorSplit(rasterFile, vectorPath, tileStart, tileEnd, pixelsPerTile);
            splits.add(split);
            tileStart = tileEnd + 1;
          }
        }
      }
      return splits;
    }
  }

  @Override
  public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return null;
  }
}
