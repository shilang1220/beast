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
package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.util.IConfigurable;
import edu.ucr.cs.bdlab.beast.util.OperationException;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.beast.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Stack;

/**
 * Writes the output files as configured by the user
 * 根据用户参数创建写格式器的类
 */
public class SpatialOutputFormat extends FileOutputFormat implements IConfigurable {

  /**The configuration for the class name of the FeatureWriterClass*/
  public static final String FeatureWriterClass = "SpatialOutputFormat.FeatureWriterClass";
  @OperationParam(
      description = "The format of the output file {point(xcol,ycol),envelope(x1col,y1col,x2col,y2col),wkt(gcol)}\n" +
          "\tpoint(xcol,ycol): indicates a CSV input where xcol and ycol indicate the indexes of the columns that" +
          "contain the x and y coordinates\n" +
          "\tenvelope(x1col,y1col,x2col,y2col): indicate an input that contains rectangles stores in (x1,y1,x2,y2) format\n" +
          "\twkt(gcol): indicate a CSV file with the field (gcol) containing a WKT-encoded geometry.\n" +
          "\tshapefile: Esri shapefile. Accepts both .shp+.shx+.dbf files or a compressed .zip file with these three files\n" +
          "\trtree: An optimized R-tree index"
  )
  public static final String OutputFormat = "oformat";
  @OperationParam(
      description = "Overwrite the output if it already exists {true, false}.",
      defaultValue = "false"
  )
  public static final String OverwriteOutput = "overwrite";

  public static Class<? extends FeatureWriter> getFeatureWriterClass(String oFormat) {
    Class<? extends FeatureWriter> writerClass;
    if (CSVFeatureReader.recognize(oFormat)) {
      // CSVFeatureReader was able to detect the input format.
      writerClass = CSVFeatureWriter.class;
    } else {
      writerClass = FeatureWriter.featureWriters.get(oFormat);
    }
    return writerClass;
  }

  /**
   * Returns the feature writer class configured in the given configuration.
   * It uses the following order to get the writer class:
   * <ol>
   *   <li>A class configured in {@link #FeatureWriterClass}</li>
   *   <li>A short name in {@link #OutputFormat}</li>
   *   <li>Assume input/output format of the same type and use {@link SpatialFileRDD#InputFormat()}</li>
   *   <li>If none of these are set, a {@code null} is returned.</li>
   * </ol>
   * @param conf the job configuration
   * @return the class of the feature writer
   */
  public static Class<? extends FeatureWriter> getConfiguredFeatureWriterClass(Configuration conf) {
    Class<? extends FeatureWriter> writerClass = conf.getClass(FeatureWriterClass, null, FeatureWriter.class);
    if (writerClass == null) {
      // Try short name for the output format
      String oFormat = conf.get(OutputFormat, conf.get(SpatialFileRDD.InputFormat()));
      writerClass = FeatureWriter.featureWriters.get(oFormat);
      if (writerClass == null && CSVFeatureReader.recognize(oFormat)) {
        // CSVFeatureReader was able to detect the input format.
        writerClass = CSVFeatureWriter.class;
      }
      conf.setClass(FeatureWriterClass, writerClass, FeatureWriter.class);
    }
    return writerClass;
  }

  @Override
  public FeatureWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    FeatureWriter writer = createRecordWriter(conf);
    // Initialize the record writer
    FeatureWriter.Metadata writerMetadata = writer.getClass().getAnnotation(FeatureWriter.Metadata.class);
    Path out = this.getDefaultWorkFile(taskAttemptContext, writerMetadata != null? writerMetadata.extension() : "");
    writer.initialize(out, conf);
    return writer;
  }

  /**
   * Create an uninitialized record writer
   * 根据参数配置，创建记录写器
   * @param conf the system configuration
   * @return the configured feature writer
   */
  protected FeatureWriter createRecordWriter(Configuration conf) {
    try {
      Class<? extends FeatureWriter> featureWriterClass = SpatialOutputFormat.getConfiguredFeatureWriterClass(conf);
      FeatureWriter writer = featureWriterClass.newInstance();
      return writer;
    } catch (InstantiationException e) {
      throw new RuntimeException(String.format("Error instantiating class '%s'", conf.get(FeatureWriterClass)), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(String.format("Cannot access the constructor of class '%s'", conf.get(FeatureWriterClass)), e);
    }
  }

  /**
   * Sets the given user-friendly output format in the configuration and sets the corresponding   *
   * FeatureWriterClass in the configuration.
   * 创建
   * @param conf the system configuration
   * @param oFormat the user-friendly input format string
   */
  public static void setOutputFormat(Configuration conf, String oFormat) {
    Class<? extends FeatureWriter> writerClass;
    conf.set(SpatialOutputFormat.OutputFormat, oFormat);
    writerClass = getFeatureWriterClass(oFormat);

    // 如果未能成功创建输出器，则根据用户输入的字符串自动推断输出格式
    if (writerClass == null) {
      int i1 = oFormat.indexOf('(');
      if (i1 != -1)
        oFormat = oFormat.substring(0, i1);
      String errorMessage = String.format("Unrecognized output format '%s'", oFormat);
      // Try to suggest some corrections
      List<String> possibleCorrections = new ArrayList<>();
      possibleCorrections.addAll(FeatureWriter.featureWriters.keySet());
      possibleCorrections.add("point");
      possibleCorrections.add("envelope");
      possibleCorrections.add("wkt");
      Collection<String> suggestions = StringUtil.nearestLevenshteinDistance(oFormat, possibleCorrections, 2);
      if (!suggestions.isEmpty())
        errorMessage += String.format(". Did you mean %s instead of '%s'",
            StringUtil.humandReable(suggestions, "or"), oFormat);
      throw new OperationException(errorMessage);
    }

    conf.setClass(FeatureWriterClass, writerClass, FeatureWriter.class);
  }

  /**
   * Adds the FeatureWriter class assigned in the user options to the list of classes with parameters
   * @param opts user options
   * @param parameterClasses (output) the dependent classes will be added to this list
   */
  @Override
  public void addDependentClasses(BeastOptions opts, Stack<Class<?>> parameterClasses) {
    if (opts == null)
      return;
    String oFormat = opts.getString(OutputFormat, opts.getString(SpatialFileRDD.InputFormat()));
    if (oFormat == null)
      return;
    if (oFormat.equals("*auto*")) {
      Tuple2<Class<? extends FeatureReader>, BeastOptions> detected =
          SpatialFileRDD.autodetectInputFormat(null, new BeastOptions());
      if (detected == null)
        throw new OperationException("Failed to auto-detect input format");
      BeastOptions detectedOptions = detected._2;
      // If an input format is detected, use it also as an output format
      opts.mergeWith(detectedOptions);
      opts.set(OutputFormat, oFormat = opts.getString(SpatialFileRDD.InputFormat()));
    }
    // TODO can we remove the next line to reflect that the output format was not explicitly set by the user?
    opts.set(SpatialOutputFormat.OutputFormat, oFormat);
    Class<? extends FeatureWriter> writerClass = opts.getClass(FeatureWriterClass, null, FeatureWriter.class);
    if (writerClass != null)
      parameterClasses.push(writerClass);
  }

}
