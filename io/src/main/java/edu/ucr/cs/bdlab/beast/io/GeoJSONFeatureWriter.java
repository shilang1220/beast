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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes {@link IFeature} values in GeoJSON format as explained in
 * <a href="https://geojson.org/">https://geojson.org/</a> and
 * <a href="http://wiki.geojson.org/">http://wiki.geojson.org/</a>.
 * The output is a file with one object that contains an attribute "FeatureCollection" that has all the
 * features in an array as separate objects.
 */
@FeatureWriter.Metadata(extension = ".geojson", shortName = "geojson")
public class GeoJSONFeatureWriter extends FeatureWriter {

  /**Whether to print the output using the pretty printer or not*/
  @OperationParam(
      description = "Set this flag to true to use the pretty printer",
      defaultValue = "true"
  )
  public static final String UsePrettyPrinter = "GeoJSONFeatureWriter.UsePrettyPrinter";

  /**The output stream*/
  protected OutputStream out;

  /**The JSON output stream writer*/
  protected JsonGenerator jsonGenerator;

  @Override
  public void initialize(Path geoJSONPath, Configuration conf) throws IOException {
    FileSystem fs = geoJSONPath.getFileSystem(conf);
    out = fs.create(geoJSONPath);
    this.initialize(out, conf);
  }

  @Override
  public void initialize(OutputStream out, Configuration conf) throws IOException {
    jsonGenerator = new JsonFactory().createGenerator(out);
    //for pretty printing
    if (conf.getBoolean(UsePrettyPrinter, true))
      jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());
    writeHeader(jsonGenerator);
  }

  /**
   * Writes the header of the GeoJSON file before any features are written
   * @param jsonGenerator the JSON generator that writes the output
   * @throws IOException if an error happens while writing the outpput
   */
  protected void writeHeader(JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeStringField("type", "FeatureCollection");
    jsonGenerator.writeFieldName("features");
    jsonGenerator.writeStartArray();
  }

  @Override
  public void write(Object dummy, IFeature f) throws IOException, InterruptedException {
   writeFeature(jsonGenerator, f);
  }

  public static void writeFeature(JsonGenerator jsonGenerator, IFeature feature) {
    try {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("type", "Feature");
      if (feature.getNumAttributes() > 0) {
        jsonGenerator.writeFieldName("properties");
        jsonGenerator.writeStartObject();
        for (int iAttr = 0; iAttr < feature.getNumAttributes(); iAttr++) {
          Object value = feature.getAttributeValue(iAttr);
          if (value != null) {
            String name = feature.getAttributeName(iAttr);
            if (name == null || name.length() == 0)
              name = String.format("attr%d", iAttr);
            // TODO write numeric data as numeric not string
            jsonGenerator.writeStringField(name, value.toString());
          }
        }
        jsonGenerator.writeEndObject();
      }
      // Write the geometry
      Geometry geom = feature.getGeometry();
      if (geom != null && !geom.isEmpty()) {
        jsonGenerator.writeFieldName("geometry");
        writeGeometryValue(jsonGenerator, geom);
      }
      jsonGenerator.writeEndObject();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error writing the feature '%s'", feature.toString()), e);
    }
  }

  /**
   * Writes a single geometry value in GeoJSON format using the given JSON generator (writer).
   *
   * @param jsonGenerator the JSON generator to write the output
   * @param geom the geometry to write in GeoJSON
   * @throws IOException if an error happens while writing the output
   * @see <a href="http://wiki.geojson.org/GeoJSON_draft_version_6">http://wiki.geojson.org/GeoJSON_draft_version_6</a>
   */
  public static void writeGeometryValue(JsonGenerator jsonGenerator, Geometry geom) throws IOException {
    jsonGenerator.writeStartObject();
    // Write field type
    String strType = null;
    switch (geom.getGeometryType()) {
      case "Point": strType = "Point"; break;
      case "LineString": strType = "LineString"; break;
      case "Envelope": // Treat as a polygon
      case "Polygon": strType = "Polygon"; break;
      case "MultiPoint": strType = "MultiPoint"; break;
      case "MultiLineString": strType = "MultiLineString"; break;
      case "MultiPolygon": strType = "MultiPolygon"; break;
      case "GeometryCollection": strType = "GeometryCollection"; break;
    }
    jsonGenerator.writeStringField("type", strType);
    // Write field value
    PointND point;
    switch (geom.getGeometryType()) {
      case "Point":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#Point
        jsonGenerator.writeFieldName("coordinates");
        writePoint(jsonGenerator, geom.getCoordinate());
        break;
      case "LineString":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#LineString
        LineString linestring = (LineString) geom;
        jsonGenerator.writeFieldName("coordinates");
        jsonGenerator.writeStartArray();
        for (int $i = 0; $i < linestring.getNumPoints(); $i++) {
          writePoint(jsonGenerator, linestring.getCoordinateN($i));
        }
        jsonGenerator.writeEndArray();
        break;
      case "Envelope":
        // GeoJSON does not support envelopes as a separate geometry. So, we write it as a polygon
        EnvelopeND envelope = (EnvelopeND) geom;
        jsonGenerator.writeFieldName("coordinates");
        jsonGenerator.writeStartArray(); // Start of polygon
        jsonGenerator.writeStartArray(); // Start of the single linear ring inside the polygon

        // first point
        jsonGenerator.writeStartArray();
        jsonGenerator.writeNumber(envelope.getMinCoord(0));
        jsonGenerator.writeNumber(envelope.getMinCoord(1));
        jsonGenerator.writeEndArray();

        // second point
        jsonGenerator.writeStartArray();
        jsonGenerator.writeNumber(envelope.getMaxCoord(0));
        jsonGenerator.writeNumber(envelope.getMinCoord(1));
        jsonGenerator.writeEndArray();

        // third point
        jsonGenerator.writeStartArray();
        jsonGenerator.writeNumber(envelope.getMaxCoord(0));
        jsonGenerator.writeNumber(envelope.getMaxCoord(1));
        jsonGenerator.writeEndArray();

        // fourth point
        jsonGenerator.writeStartArray();
        jsonGenerator.writeNumber(envelope.getMinCoord(0));
        jsonGenerator.writeNumber(envelope.getMaxCoord(1));
        jsonGenerator.writeEndArray();

        // fifth point (= first point)
        jsonGenerator.writeStartArray();
        jsonGenerator.writeNumber(envelope.getMinCoord(0));
        jsonGenerator.writeNumber(envelope.getMinCoord(1));
        jsonGenerator.writeEndArray();

        jsonGenerator.writeEndArray(); // End of the linear ring
        jsonGenerator.writeEndArray(); // End of the polygon
        break;
      case "Polygon":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#Polygon
        Polygon polygon = (Polygon) geom;
        jsonGenerator.writeFieldName("coordinates");
        // Start the array of rings
        jsonGenerator.writeStartArray();
        for (int $iRing = 0; $iRing < polygon.getNumInteriorRing() + 1; $iRing++) {
          // String the array of points in this ring
          jsonGenerator.writeStartArray();
          LineString ring = $iRing == 0? polygon.getExteriorRing() : polygon.getInteriorRingN($iRing - 1);
          for (int $iPoint = 0; $iPoint < ring.getNumPoints(); $iPoint++)
            writePoint(jsonGenerator, ring.getCoordinateN($iPoint));
          // Close the array of points in this ring
          jsonGenerator.writeEndArray();
        }
        // Close the array of rings
        jsonGenerator.writeEndArray();
        break;
      case "MultiPoint":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPoint
        jsonGenerator.writeFieldName("coordinates");
        jsonGenerator.writeStartArray();
        for (int iPoint = 0; iPoint < geom.getNumGeometries(); iPoint++)
          writePoint(jsonGenerator, geom.getGeometryN(iPoint).getCoordinate());
        jsonGenerator.writeEndArray(); // End coordinates array
        break;
      case "MultiLineString":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiLineString
        MultiLineString multiLineString = (MultiLineString) geom;
        jsonGenerator.writeFieldName("coordinates");
        jsonGenerator.writeStartArray();
        for (int iLineString = 0; iLineString < multiLineString.getNumGeometries(); iLineString++) {
          jsonGenerator.writeStartArray();
          LineString subls = (LineString) multiLineString.getGeometryN(iLineString);
          for (int $iPoint = 0; $iPoint < subls.getNumPoints(); $iPoint++) {
            writePoint(jsonGenerator, subls.getCoordinateN($iPoint));
          }
          jsonGenerator.writeEndArray(); // End sub-linestring
        }
        jsonGenerator.writeEndArray(); // End coordinates array
        break;
      case "MultiPolygon":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPolygon
        MultiPolygon multiPolygon = (MultiPolygon) geom;
        jsonGenerator.writeFieldName("coordinates");
        jsonGenerator.writeStartArray(); // Start of the multipolygon
        for (int $iPoly = 0; $iPoly < multiPolygon.getNumGeometries(); $iPoly++) {
          jsonGenerator.writeStartArray(); // Start of the polygon
          Polygon subpoly = (Polygon) multiPolygon.getGeometryN($iPoly);
          // Write exterior ring
          for (int $iRing = 0; $iRing < subpoly.getNumInteriorRing() + 1; $iRing++) {
            jsonGenerator.writeStartArray(); // Start of the ring
            LinearRing ring = (LinearRing) ($iRing == 0? subpoly.getExteriorRing() : subpoly.getInteriorRingN($iRing - 1));
            for (int $iPoint = 0; $iPoint < ring.getNumPoints(); $iPoint++) {
              // Write the point
              writePoint(jsonGenerator, ring.getCoordinateN($iPoint));
            }
            // Close the array of points in the current ring
            jsonGenerator.writeEndArray(); // End of the current ring
          }
          jsonGenerator.writeEndArray(); // End of the current polygon
        }
        jsonGenerator.writeEndArray(); // End of the multipolygon
        break;
      case "GeometryCollection":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#GeometryCollection
        GeometryCollection geometryCollection = (GeometryCollection) geom;
        jsonGenerator.writeFieldName("geometries");
        jsonGenerator.writeStartArray(); // Start of the geometry collection
        for (int $iGeom = 0; $iGeom < geometryCollection.getNumGeometries(); $iGeom++) {
          writeGeometryValue(jsonGenerator, (geometryCollection.getGeometryN($iGeom)));
        }
        jsonGenerator.writeEndArray(); // End of the geometry collection
        break;
      default:
        throw new RuntimeException(String.format("Geometry type '%s' is not yet supported in GeoJSON", geom.getGeometryType()));
    }
    jsonGenerator.writeEndObject();
  }

  private static void writePoint(JsonGenerator jsonGenerator, Coordinate p) throws IOException {
    jsonGenerator.writeStartArray();
    jsonGenerator.writeNumber(p.getX());
    jsonGenerator.writeNumber(p.getY());
    jsonGenerator.writeEndArray();
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException {
    // Close the array of features
    jsonGenerator.writeEndArray();
    // Close the main object
    jsonGenerator.writeEndObject();
    jsonGenerator.close();
  }
}
