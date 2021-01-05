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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A record reader that reads CSV file with custom field delimiter
 */
@FeatureReader.Metadata(
    description = "Parses a GepJSON file as one record for each feature object",
    shortName = "geojson",
    extension = ".geojson"
)
public class GeoJSONFeatureReader extends FeatureReader {
  private static final Log LOG = LogFactory.getLog(GeoJSONFeatureReader.class);

  /**The mutable feature*/
  protected Feature feature;

  /**An optional attributed to filter the geometries in the input file*/
  private EnvelopeND filterMBR;

  /**The start of the split*/
  protected long start;

  /**The end of the split*/
  protected long end;

  /**The input stream to the raw file (compressed)*/
  protected FSDataInputStream fileIn;

  /**The input stream to the decompressed file*/
  protected InputStream in;

  /**The current position in the file. Either the raw file or the comrpessed file*/
  protected Seekable filePosition;
  protected boolean isCompressedInput;
  protected Decompressor decompressor;

  /**The JSON parser that reads json tokens*/
  private JsonParser jsonParser;

  /**A flag that is raised when end-of-split is reached*/
  protected boolean eos;

  @Override
  public void initialize(InputSplit genericSplit, Configuration conf) throws IOException {
    // Open the input split and decompress if necessary
    FileSplit split = (FileSplit) genericSplit;
    this.initialize(split.getPath(), split.getStart(), split.getLength(), conf);
  }

  /**
   * An initializer that can be used outside the regular MapReduce context.
   * @param inputFile the path of the input file
   * @param conf the system configuration
   * @throws IOException if an error happens while opening the input file
   */
  public void initialize(Path inputFile, Configuration conf) throws IOException {
    FileStatus fileStatus = inputFile.getFileSystem(conf).getFileStatus(inputFile);
    this.initialize(inputFile, 0, fileStatus.getLen(), conf);
  }

  /**
   * An internal initializer that takes a file path, a start and length.
   * @param file path to the file to open. Works with both compressed and decompressed files (based on extension)
   * @param start the starting offset to parse
   * @param length the number of bytes to parse
   * @param conf the environment configuration
   * @throws IOException if an error happens while opening the input file
   */
  protected void initialize(Path file, long start, long length, Configuration conf) throws IOException {
    this.start = start;
    this.eos = length == 0;
    this.end = start + length;

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(conf);
    fileIn = fs.open(file);

    CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
    if (null != codec) {
      // The file is compressed. Decompress it on the fly
      isCompressedInput = true;
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        // Can decompress a small part of the file
        final SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).createInputStream(
            fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
        this.start = cIn.getAdjustedStart();
        this.end = cIn.getAdjustedEnd();
        this.filePosition = cIn;
        this.in = cIn;
      } else {
        // Need to decompress the entire file from the beginning
        final CompressionInputStream cIn = codec.createInputStream(fileIn, decompressor);
        this.in = cIn;
        this.filePosition = fileIn;
      }
    } else {
      // Not a compressed file
      fileIn.seek(start);
      filePosition = fileIn;
      in = fileIn;
    }

    JsonFactory jsonFactory = new JsonFactory();
    jsonParser = new SilentJsonParser(jsonFactory.createParser(in));

    // Retrieve the filter MBR
    String filterMBRStr = conf.get(SpatialFileRDD.FilterMBR());
    if (filterMBRStr != null) {
      String[] parts = filterMBRStr.split(",");
      double[] dblParts = new double[parts.length];
      for (int i = 0; i < parts.length; i++)
        dblParts[i] = Double.parseDouble(parts[i]);
      this.filterMBR = new EnvelopeND(DefaultGeometryFactory, dblParts.length/2, dblParts);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (eos)
      return false;
    // The assumption is that the underlying parser now points to the next token after the "type": "feature"
    // The assumption also is that the "type": "feature" token appears before the geometry and properties
    // Create a new value since Spark does not support mutable objects

    JsonToken token;
    // Read until the beginning of the next feature
    boolean recordFound = false;
    long posOfLastStartObject = this.getFilePosition();
    while ((token = jsonParser.nextToken()) != null && posOfLastStartObject < this.end) {
      if (token == JsonToken.FIELD_NAME) {
        String fieldName = jsonParser.getCurrentName();
        if (fieldName.equalsIgnoreCase("type")) {
          String fieldValue = jsonParser.nextTextValue();
          if (fieldValue.equalsIgnoreCase("feature")) {
            recordFound = true;
            break;
          }
        }
      } else if (token == JsonToken.START_OBJECT) {
        posOfLastStartObject = this.getFilePosition();
      }
    }
    eos = !recordFound;
    // Now, read the actual feature
    Geometry geometry = null;
    List<String> names = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    while (!eos && (token = jsonParser.nextToken()) != JsonToken.END_OBJECT) {
      if (token == JsonToken.FIELD_NAME && jsonParser.getCurrentName().equalsIgnoreCase("geometry")) {
        consumeAndCheckToken(JsonToken.START_OBJECT);
        geometry = readGeometry();
      } else if (token == JsonToken.FIELD_NAME && jsonParser.getCurrentName().equalsIgnoreCase("properties")) {
        readProperties(names, values);
      } else if (token == JsonToken.FIELD_NAME) {
        // Set additional attributes
        names.add(jsonParser.getCurrentName());
        values.add(jsonParser.nextTextValue());
      } else if (token == null) {
        eos = true;
      }
    }
    feature = Feature.create(geometry, names.toArray(new String[0]), null, values.toArray());
    return !eos;
  }

  /**
   * Read properties as key-value pairs. It starts by reading the start object, the attributes as key-value pairs, and
   * finally the end object.
   * @param names (out) the names of the properties parsed from the input
   * @param values (out) the values of the properties parsed from the input
   * @throws IOException if an error happens while reading the input
   */
  protected void readProperties(List<String> names, List<Object> values) throws IOException {
    consumeAndCheckToken(JsonToken.START_OBJECT);
    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      String key = jsonParser.getCurrentName();
      JsonToken token = jsonParser.nextToken();
      Object value;
      switch (token) {
        case VALUE_FALSE: value = Boolean.FALSE; break;
        case VALUE_TRUE: value = Boolean.TRUE; break;
        case VALUE_STRING: value = jsonParser.getText(); break;
        case VALUE_NUMBER_INT: value = jsonParser.getLongValue(); break;
        case VALUE_NUMBER_FLOAT: value = jsonParser.getDoubleValue(); break;
        default:
          throw new RuntimeException(String.format("Unsupported value type '%s'", token));
      }
      names.add(key);
      values.add(value);
    }
  }

  /**
   * Reads a geometry object from the JsonParser. The assumption is that the start object token of the geometry has
   * already been consumed. This function should read and consume the end object of the geometry
   * @return the given geometry if it was reused or a new geometry object otherwise.
   * @throws IOException if an error happens while reading the input
   */
  protected Geometry readGeometry() throws IOException {
    Geometry geom;
    // Clear the existing geometry to prepare it for possible reuse
    consumeAndCheckFieldName("type");
    String geometryType = jsonParser.nextTextValue().toLowerCase();
    List<Coordinate> coordinates;
    List<Geometry> parts;
    Geometry jtsGeom;
    CoordinateXY coordinate = new CoordinateXY();
    switch (geometryType) {
      case "point":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#Point
        consumeAndCheckFieldName("coordinates");
        consumeAndCheckToken(JsonToken.START_ARRAY);
        consumeNumber();
        coordinate.setX(jsonParser.getDoubleValue());
        consumeNumber();
        coordinate.setY(jsonParser.getDoubleValue());
        consumeAndCheckToken(JsonToken.END_ARRAY);
        geom = DefaultGeometryFactory.createPoint(coordinate);
        break;
      case "linestring":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#LineString
        coordinates = new ArrayList<>();
        consumeAndCheckFieldName("coordinates");
        consumeAndCheckToken(JsonToken.START_ARRAY);
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
          consumeNumber();
          double x = jsonParser.getDoubleValue();
          consumeNumber();
          double y = jsonParser.getDoubleValue();
          coordinates.add(new CoordinateXY(x, y));
          consumeAndCheckToken(JsonToken.END_ARRAY);
        }
        jtsGeom = DefaultGeometryFactory.createLineString(coordinates.toArray(new Coordinate[0]));
        geom = (jtsGeom);
        break;
      case "polygon":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#Polygon
        parts = new ArrayList<>();
        consumeAndCheckFieldName("coordinates");
        consumeAndCheckToken(JsonToken.START_ARRAY);
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
          coordinates = new ArrayList<>();
          // Read one linear ring
          while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            consumeNumber();
            double x = jsonParser.getDoubleValue();
            consumeNumber();
            double y = jsonParser.getDoubleValue();
            coordinates.add(new CoordinateXY(x, y));
            consumeAndCheckToken(JsonToken.END_ARRAY);
          }
          parts.add(DefaultGeometryFactory.createLinearRing(coordinates.toArray(new Coordinate[0])));
          coordinates.clear();
        }
        LinearRing shell = (LinearRing) parts.remove(0);
        LinearRing[] holes = parts.toArray(new LinearRing[0]);
        jtsGeom = DefaultGeometryFactory.createPolygon(shell, holes);
        geom = (jtsGeom);
        break;
      case "multipoint":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPoint
        List<Point> points = new ArrayList<>();
        consumeAndCheckFieldName("coordinates");
        consumeAndCheckToken(JsonToken.START_ARRAY);
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
          consumeNumber();
          double x = jsonParser.getDoubleValue();
          consumeNumber();
          double y = jsonParser.getDoubleValue();
          points.add(DefaultGeometryFactory.createPoint(new Coordinate(x, y)));
          consumeAndCheckToken(JsonToken.END_ARRAY);
        }
        geom = DefaultGeometryFactory.createMultiPoint(points.toArray(new Point[0]));
        break;
      case "multilinestring":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiLineString
        consumeAndCheckFieldName("coordinates");
        consumeAndCheckToken(JsonToken.START_ARRAY);
        parts = new ArrayList<>();
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
          coordinates = new ArrayList<>();
          // Read one line string
          while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            consumeNumber();
            double x = jsonParser.getDoubleValue();
            consumeNumber();
            double y = jsonParser.getDoubleValue();
            coordinates.add(new CoordinateXY(x, y));
            consumeAndCheckToken(JsonToken.END_ARRAY);
          }
          parts.add(DefaultGeometryFactory.createLineString(coordinates.toArray(new Coordinate[0])));
          coordinates.clear();
        }
        jtsGeom = DefaultGeometryFactory.createMultiLineString(parts.toArray(new LineString[0]));
        geom = (jtsGeom);
        break;
      case "multipolygon":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPolygon
        consumeAndCheckFieldName("coordinates");
        consumeAndCheckToken(JsonToken.START_ARRAY);
        List<Polygon> polygons = new ArrayList<>();
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
          // Read one polygon
          parts = new ArrayList<>();
          while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            // Read one linear ring
            coordinates = new ArrayList<>();
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
              consumeNumber();
              double x = jsonParser.getDoubleValue();
              consumeNumber();
              double y = jsonParser.getDoubleValue();
              coordinates.add(new CoordinateXY(x, y));
              consumeAndCheckToken(JsonToken.END_ARRAY);
            }
            // Done with one linear ring
            parts.add(DefaultGeometryFactory.createLinearRing(coordinates.toArray(new Coordinate[0])));
            coordinates.size();
          }
          // Done with one polygon
          shell = (LinearRing) parts.remove(0);
          holes = parts.toArray(new LinearRing[0]);
          polygons.add(DefaultGeometryFactory.createPolygon(shell, holes));
          parts.clear();
        }
        // Done with the multipolygon
        jtsGeom = DefaultGeometryFactory.createMultiPolygon(polygons.toArray(new Polygon[0]));
        geom = (jtsGeom);
        break;
    case "geometrycollection":
      // http://wiki.geojson.org/GeoJSON_draft_version_6#GeometryCollection
      List<Geometry> geoms = new ArrayList<>();
      consumeAndCheckFieldName("geometries");
      consumeAndCheckToken(JsonToken.START_ARRAY);
      while (jsonParser.nextToken() != JsonToken.END_ARRAY)
        geoms.add((readGeometry()));
      geom = DefaultGeometryFactory.createGeometryCollection(geoms.toArray(new Geometry[0]));
      break;
    default:
      throw new RuntimeException(String.format("Unexpected geometry type '%s'", geometryType));
    }
    consumeAndCheckToken(JsonToken.END_OBJECT);
    return geom;
  }

  /**
   * Read the next token and ensure it is a numeric token. Either Integer or Float
   */
  private void consumeNumber() throws IOException {
    JsonToken actual = jsonParser.nextToken();
    if (actual != JsonToken.VALUE_NUMBER_FLOAT && actual != JsonToken.VALUE_NUMBER_INT) {
      // Throw a parse exception.
      // TODO use a specialized exception(s)
      int lineNumber = jsonParser.getTokenLocation().getLineNr();
      int characterNumber = jsonParser.getTokenLocation().getColumnNr();
      throw new RuntimeException(String.format("Error parsing GeoJSON file. " +
          "Expected numeric value but found %s at line %d character %d", actual, lineNumber, characterNumber));
    }
  }

  private void consumeAndCheckToken(JsonToken expected) throws IOException {
    JsonToken actual = jsonParser.nextToken();
    if (actual != expected) {
      // Throw a parse exception.
      // TODO use a specialized exception(s)
      int lineNumber = jsonParser.getTokenLocation().getLineNr();
      int characterNumber = jsonParser.getTokenLocation().getColumnNr();
      throw new RuntimeException(String.format("Error parsing GeoJSON file. " +
          "Expected token %s but found %s at line %d character %d", expected, actual, lineNumber, characterNumber));
    }
  }

  private void consumeAndCheckFieldName(String expected) throws IOException {
    consumeAndCheckToken(JsonToken.FIELD_NAME);
    String actual = jsonParser.getCurrentName();
    if (!expected.equalsIgnoreCase(actual)) {
      // Throw a parse exception.
      // TODO use a specialized exception(s)
      int lineNumber = jsonParser.getTokenLocation().getLineNr();
      int characterNumber = jsonParser.getTokenLocation().getColumnNr();
      throw new RuntimeException(String.format("Error parsing GeoJSON file. " +
          "Expected field '%s' but found '%s' at line %d character %d", expected, actual, lineNumber, characterNumber));
    }
  }

  @Override
  public IFeature getCurrentValue() {
    return feature;
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = start + jsonParser.getTokenLocation().getByteOffset();
    }
    return retVal;
  }

  @Override
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }

  @Override
  public void close() throws IOException {
    jsonParser.close();
  }

}
