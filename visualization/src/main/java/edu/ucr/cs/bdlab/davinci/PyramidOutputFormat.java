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
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.FeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * An output format that is used to write either image tiles or data tiles.
 * @author Ahmed Eldawy
 *
 */
public class PyramidOutputFormat extends FileOutputFormat<Long, Object> {
  private static final Log LOG = LogFactory.getLog(PyramidOutputFormat.class);

  static class ImageRecordWriter extends RecordWriter<Long, Object> {

    /**The plotter used to write image tiles*/
    private Plotter plotter;
    /**The output file system*/
    private final FileSystem outFS;
    /**A path to a directory that will contain all the files*/
    private final Path outPath;
    /**Flip images vertically and adjust their position in the pyramid accordingly*/
    private boolean vflip;
    /**Used to indicate progress to Hadoop*/
    private TaskAttemptContext context;
    /**Extension of output images*/
    private String imgExt;

    /**The ID of the data tile that is currently being written*/
    private long currentDataTileID;

    /**One record writer that writes to the current data tile.*/
    private FeatureWriter currentDataTileWriter;

    /**A temporary tile index to decocde the tile ID*/
    private TileIndex tempTileIndex;

    /**The output format used to write all data output*/
    private final SpatialOutputFormat dataTileOutput;

    ImageRecordWriter(FileSystem outFs, Path taskOutPath, TaskAttemptContext context) {
      this.context = context;
      Configuration conf = context.getConfiguration();
      // This line sometimes helps with creating the Graphics2D without an x11 display
      System.setProperty("java.awt.headless", "true");
      this.plotter = Plotter.createAndConfigurePlotter(new BeastOptions(conf));
      this.outPath = taskOutPath;
      this.outFS = outFs;
      this.vflip = conf.getBoolean(CommonVisualizationHelper.VerticalFlip, true);
      imgExt = Plotter.getImageExtension(this.plotter.getClass());
      // Invalidate the ID of the current data tile
      currentDataTileID = 0;

      dataTileOutput = new SpatialOutputFormat() {
        @Override
        public FeatureWriter getRecordWriter(TaskAttemptContext context) throws IOException {
          // Create an uninitialized record writer to avoid creating a default output file
          return createRecordWriter(context.getConfiguration());
        }
      };
    }

    private final Path getTilePath(int z, int x, int y, String ext) {
      if (vflip)
        y = ((1 << z) - 1) - y;
      return new Path(outPath, MultilevelPyramidPlotHelper.getTileFileName(z, x, y)+ext);
    }

    @Override
    public void write(Long encodedTileID, Object w) throws IOException, InterruptedException {
      tempTileIndex = TileIndex.decode(encodedTileID, tempTileIndex);
      if (w instanceof Canvas) {
        Path imagePath = getTilePath(tempTileIndex.z, tempTileIndex.x, tempTileIndex.y, imgExt);
        // Write this tile as an image
        FSDataOutputStream outFile = outFS.create(imagePath);
        plotter.writeImage((Canvas) w, outFile, this.vflip);
        outFile.close();
      } else if (w instanceof IFeature) {
        // Write the feature to a regular file

        // Check if we already have an output file for this tile
        if (encodedTileID != currentDataTileID) {
          if (currentDataTileWriter != null)
            currentDataTileWriter.close(this.context);
          currentDataTileWriter = dataTileOutput.getRecordWriter(context);
          // Initialize the record writer on the data file path
          String extension = currentDataTileWriter.getClass().getAnnotation(FeatureWriter.Metadata.class).extension();
          Path filePath = getTilePath(tempTileIndex.z, tempTileIndex.x, tempTileIndex.y, extension);
          currentDataTileWriter.initialize(filePath, context.getConfiguration());
          currentDataTileID = encodedTileID;
        }
        currentDataTileWriter.write(null, (IFeature) w);
      }
      context.progress();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      // If there is an open data tile writer, close it
      if (currentDataTileWriter != null) {
        currentDataTileWriter.close(this.context);
        currentDataTileWriter = null;
      }
    }
  }
  
  @Override
  public RecordWriter<Long, Object> getRecordWriter(TaskAttemptContext task) throws IOException {
    Path file = getDefaultWorkFile(task, "").getParent();
    FileSystem fs = file.getFileSystem(task.getConfiguration());
    return new ImageRecordWriter(fs, file, task);
  }
  
}
