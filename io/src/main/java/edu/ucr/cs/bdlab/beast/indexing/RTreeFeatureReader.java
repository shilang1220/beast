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
import edu.ucr.cs.bdlab.beast.geolite.FieldType;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.util.BitArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.beast.CRSServer;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import java.util.Iterator;

/**
 * Reads features from an R-tree-indexed file.
 */
@FeatureReader.Metadata(
    description = "An R-tree locally indexed file for efficient range retrieval",
    shortName = "rtree",
    extension = ".rtree",
    noSplit = true
)
public class RTreeFeatureReader extends FeatureReader {

  /**A mutable key for all records*/
  protected EnvelopeND key;

  /**The value that is returned*/
  protected IFeature value;

  protected Iterator<? extends IFeature> results;

  /**The file name to report in error messages*/
  private String filename;

  /**The geometry reader to read geometries from the R-tree. Configured to use the right SRID.*/
  private GeometryReader reader;

  /**The input to the file*/
  private FSDataInputStream in;

  /**The start position of the current tree*/
  private long posCurrentTree;

  /**The position of the start tree*/
  private long posFirstTree;

  /**The deserializer reads records from the R-tree*/
  private RTreeGuttman.Deserializer<Feature> featureDeserializer;

  /**If the input should be filtered, these are the search coordinates*/
  private double[] minCoord;
  private double[] maxCoord;

  @Override
  public void initialize(InputSplit split, Configuration conf) throws IOException {
    FileSplit fsplit = (FileSplit) split;

    // Open the input file and read the header of the stored features
    filename = fsplit.getPath().toString();
    FileSystem fs = fsplit.getPath().getFileSystem(conf);
    in = fs.open(fsplit.getPath());
    in.seek(fsplit.getStart());
    Tuple2<FieldType[], String[]> header = readFeatureHeader(in);

    String wkt = in.readUTF();
    int srid;
    if (wkt.isEmpty())
      srid = 0;
    else {
      try {
        CoordinateReferenceSystem crs = CRS.parseWKT(wkt);
        srid = CRSServer.crsToSRID(crs, CRSServer.sparkConfFromHadoopConf(conf));
      } catch (FactoryException e) {
        srid = 4326;
      }
    }
    reader = GeometryReader.getGeometryReader(srid);

    // The current position is where the reading should stop (starting from the end)
    posFirstTree = in.getPos();
    posCurrentTree = fsplit.getStart() + fsplit.getLength();

    // Now, either read the entire file, or filter based on the MBR
    String filterMBRStr = conf.get(SpatialFileRDD.FilterMBR());
    if (filterMBRStr != null) {
      // Filter based on the MBR
      String[] parts = filterMBRStr.split(",");
      assert parts.length % 2 == 0; // It has to be an even number
      int numDimensions = parts.length / 2;
      minCoord = new double[numDimensions];
      maxCoord = new double[numDimensions];
      for (int d$ = 0; d$ < numDimensions; d$++) {
        minCoord[d$] = Double.parseDouble(parts[d$]);
        maxCoord[d$] = Double.parseDouble(parts[numDimensions + d$]);
      }
    }
    // Create the deserializer of geometries
    featureDeserializer = input -> {
      try {
        return readFeatureValue(input, header, reader);
      } catch (Exception e) {
        throw new RuntimeException("Error reading feature from file "+filename, e);
      }
    };
   readPreviousRTree();
  }

  /**
   * Read the previous R-tree. The file is read form the end to the beginning.
   */
  private void readPreviousRTree() throws IOException {
    assert posCurrentTree > posFirstTree :
        String.format("Cannot seek before tree at position %d while the start is at %d", posCurrentTree, posFirstTree);
    // Get the tree length by subtracting the Feature header size
    in.seek(posCurrentTree - 4);
    int treeLength = in.readInt() + 4;
    posCurrentTree -= treeLength;
    in.seek(posCurrentTree);

    if (minCoord != null) {
      // Search using the given rectangle
      results = RTreeGuttman.search(in, treeLength, minCoord, maxCoord, featureDeserializer).iterator();
    } else {
      // Read all records
      results = RTreeGuttman.readAll(in, treeLength, featureDeserializer).iterator();
    }
  }

  @Override
  public boolean nextKeyValue() {
    while (results.hasNext() || posCurrentTree > posFirstTree) {
      if (results.hasNext()) {
        value = results.next();
        if (key == null)
          key = new EnvelopeND(reader.getGeometryFactory());
        else
          key.setEmpty();
        key.merge(value.getGeometry());
        return true;
      }
      try {
        readPreviousRTree();
      } catch (IOException e) {
        throw new RuntimeException("Error reading R-tree", e);
      }
    }
    return false;
  }

  @Override
  public IFeature getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() throws IOException {
    return results instanceof RTreeGuttman.DiskSearchIterator?
        ((RTreeGuttman.DiskSearchIterator<IFeature>) results).getProgress() : 0.1f;
  }

  @Override
  public void close() throws IOException {
    if (results != null)
      ((Closeable)results).close();
  }

  /**
   * Reads and returns the header from the given input stream as a list of data types and names.
   * @param in the input stream to read from
   * @return a list of types and names or {@code null} if number of attributes is zero
   * @throws IOException
   */
  protected static Tuple2<FieldType[], String[]> readFeatureHeader(DataInput in) throws IOException {
    int numAttributes = in.readUnsignedByte();
    if (numAttributes > 0) {
      FieldType[] types = new FieldType[numAttributes];
      for (int i = 0; i < numAttributes; i++) {
        int type = in.readByte();
        switch (type) {
          case 0: types[i] = FieldType.StringType; break;
          case 1: types[i] = FieldType.IntegerType; break;
          case 2: types[i] = FieldType.LongType; break;
          case 3: types[i] = FieldType.DoubleType; break;
          case 4: types[i] = FieldType.TimestampType; break;
          case 5: types[i] = FieldType.BooleanType; break;
          default: throw new RuntimeException("Unrecognized type "+type);
        }
      }

      String[] names = new String[numAttributes];
      for (int i = 0; i < numAttributes; i++)
        names[i] = in.readUTF();

      return new Tuple2<>(types, names);
    }
    return null;
  }

  /**
   * Read the geometry and attribute values from the given input and create a new feature
   * @param in the input reader to read the data from
   * @param header the header of the feature
   * @param reader the reader that creates the geometry
   * @return the new feature that was read
   * @throws IOException if an error happens while reading the feature.
   */
  protected static Feature readFeatureValue(DataInput in, Tuple2<FieldType[], String[]> header,
                                            GeometryReader reader) throws IOException {
    int numAttributes = header == null? 0 : header._1.length;
    Object[] values = null;
    // Read all attributes (except the geometry)
    if (numAttributes > 0) {
      // Read attribute values and which ones are null
      int valueSize = in.readInt();
      byte[] valueBytes = new byte[valueSize];
      in.readFully(valueBytes);
      BitArray attributeExists = new BitArray(numAttributes);
      attributeExists.readBitsMinimal(in);

      // Parse the attribute value
      ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
      values = new Object[numAttributes];
      for (int i = 0; i < numAttributes; i++) {
        if (attributeExists.get(i)) {
          switch (header._1[i]) {
            case StringType:
              int stringLength = buffer.getShort();
              values[i] = new String(valueBytes, buffer.position(), stringLength);
              // Advance the buffer position to skip over the string
              buffer.position(buffer.position() + stringLength);
              break;
            case IntegerType:
              values[i] = buffer.getInt();
              break;
            case LongType:
              values[i] = buffer.getLong();
              break;
            case DoubleType:
              values[i] = buffer.getDouble();
              break;
            case TimestampType:
              GregorianCalendar c = new GregorianCalendar(Feature.UTC());
              c.setTimeInMillis(buffer.getLong());
              values[i] = c;
              break;
            case BooleanType:
              values[i] = buffer.get() == 1;
              break;
            default:
              throw new RuntimeException("Unsupported type " + header._1[i]);
          }
        }
      }
    }
    // Read the geometry
    Geometry geometry = reader.parse(in);
    return new Feature(geometry, header == null? null : header._2, header == null? null : header._1, values);
  }
}
