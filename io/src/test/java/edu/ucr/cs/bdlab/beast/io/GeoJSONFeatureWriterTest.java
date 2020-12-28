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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GeoJSONFeatureWriterTest extends JavaSparkTest {

  public static GeometryFactory factory = new GeometryFactory();

  public static CoordinateSequence createCoordinateSequence(double ... coordinates) {
    int size = coordinates.length / 2;
    CoordinateSequence cs = factory.getCoordinateSequenceFactory().create(coordinates.length / 2, 2);
    for (int i = 0; i < size; i++) {
      cs.setOrdinate(i, 0, coordinates[2 * i]);
      cs.setOrdinate(i, 1, coordinates[2 * i + 1]);
    }
    return cs;
  }

  public static LineString createLineString(double ... coordinates) {
    return factory.createLineString(createCoordinateSequence(coordinates));
  }

  public static Polygon createPolygonJTS(CoordinateSequence ... rings) {
    LinearRing shell = factory.createLinearRing(rings[0]);
    LinearRing[] holes = new LinearRing[rings.length - 1];
    for (int $i = 1; $i < rings.length; $i++)
      holes[$i-1] = factory.createLinearRing(rings[$i]);
    return factory.createPolygon(shell, holes);
  }

  public static Polygon createPolygon(CoordinateSequence ... rings) {
    LinearRing shell = factory.createLinearRing(rings[0]);
    LinearRing[] holes = new LinearRing[rings.length - 1];
    for (int $i = 1; $i < rings.length; $i++)
      holes[$i-1] = factory.createLinearRing(rings[$i]);
    return factory.createPolygon(shell, holes);
  }

  public void testCreationWithPoints() throws IOException, InterruptedException {
    Geometry[] geometries = {
        new PointND(new GeometryFactory(), 2, 0.0,1.0),
        new PointND(new GeometryFactory(), 2, 3.0, 10.0)
    };

    Configuration conf = sparkContext().hadoopConfiguration();
    Path outPath = new Path(scratchPath(), "test");
    Path outputFileName = new Path(outPath, "test.geojson");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    GeoJSONFeatureWriter writer = new GeoJSONFeatureWriter();
    try {
      writer.initialize(outputFileName, conf);
      for (Geometry geom : geometries) {
        Feature f = new Feature(geom, new String[] {"name"}, null, new Object[] {"name-value"});
        writer.write(null, f);
      }
    } finally {
      writer.close(null);
    }

    assertTrue("Output file not found", fileSystem.exists(outputFileName));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(new File(outputFileName.toString()));
    assertEquals("FeatureCollection", rootNode.get("type").asText());
    JsonNode features = rootNode.get("features");
    assertTrue(features.isArray());
    assertEquals(2, features.size());
    JsonNode firstFeature = features.get(0);
    assertEquals("Feature", firstFeature.get("type").asText());
    JsonNode firstGeometry = firstFeature.get("geometry");
    assertEquals("Point", firstGeometry.get("type").asText());
    assertEquals(2, firstGeometry.get("coordinates").size());
  }

  public void testWriteWithAnonymousAttributes() throws IOException, InterruptedException {
    Geometry[] geometries = {
        new PointND(new GeometryFactory(), 2, 0.0,1.0),
        new PointND(new GeometryFactory(), 2, 3.0, 10.0)
    };

    Configuration conf = sparkContext().hadoopConfiguration();
    Path outPath = new Path(scratchPath(), "test");
    Path outputFileName = new Path(outPath, "test.geojson");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    GeoJSONFeatureWriter writer = new GeoJSONFeatureWriter();
    try {
      writer.initialize(outputFileName, conf);
      for (Geometry geom : geometries) {
        Feature f = new Feature(geom, null, null, new Object[] {"name-value"});
        writer.write(null, f);
      }
    } finally {
      writer.close(null);
    }

    assertTrue("Output file not found", fileSystem.exists(outputFileName));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(new File(outputFileName.toString()));
    assertEquals("FeatureCollection", rootNode.get("type").asText());
    JsonNode features = rootNode.get("features");
    assertTrue(features.isArray());
    assertEquals(2, features.size());
    JsonNode firstFeature = features.get(0);
    assertEquals(1, firstFeature.get("properties").size());
  }

  public void testWriteWithNullAttributes() throws IOException, InterruptedException {
    Geometry[] geometries = {
        new PointND(new GeometryFactory(), 2, 0.0,1.0),
        new PointND(new GeometryFactory(), 2, 3.0, 10.0)
    };

    Configuration conf = sparkContext().hadoopConfiguration();
    Path outPath = new Path(scratchPath(), "test");
    Path outputFileName = new Path(outPath, "test.geojson");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    GeoJSONFeatureWriter writer = new GeoJSONFeatureWriter();
    try {
      writer.initialize(outputFileName, conf);
      for (Geometry geom : geometries) {
        Feature f = new Feature(geom, null, null, new Object[] {null, "field-value"});
        writer.write(null, f);
      }
    } finally {
      writer.close(null);
    }

    assertTrue("Output file not found", fileSystem.exists(outputFileName));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(new File(outputFileName.toString()));
    assertEquals("FeatureCollection", rootNode.get("type").asText());
    JsonNode features = rootNode.get("features");
    assertTrue(features.isArray());
    assertEquals(2, features.size());
    JsonNode firstFeature = features.get(0);
    assertEquals(1, firstFeature.get("properties").size());
  }

  public void testCreationWithAllGeometries() throws IOException, InterruptedException {
    List<Geometry> geometries = new ArrayList<>();
    geometries.add(new PointND(new GeometryFactory(), 2, 0.0, 1.0));

    LineString lineString = factory.createLineString(createCoordinateSequence(
          12.0, 13.5,
          15.3, -27.5));
    geometries.add(lineString);

    Polygon polygon = createPolygon(createCoordinateSequence(
          25.0, 33.0,
          45.0, 77.0,
          10.0, 88.0,
          25.0, 33.0));
    geometries.add(polygon);

    MultiLineString multiLineString = factory.createMultiLineString(new LineString[] {createLineString(
          1.0, 2.0,
          3.0, 4.0,
          1.0, 5.0), createLineString(
          11.0, 2.0,
          13.0, 4.0,
          11.0, 5.0)});
    geometries.add(multiLineString);

    MultiPolygon multiPolygon = factory.createMultiPolygon(new Polygon[] {createPolygonJTS(createCoordinateSequence(
        15.0, 33.0,
        25.0, 35.0,
        -10.0, 7.0,
        15.0, 33.0)), createPolygonJTS(createCoordinateSequence(
        115.0, 33.0,
        125.0, 35.0,
        -110.0, 7.0,
        115.0, 33.0))});
    geometries.add(multiPolygon);

    MultiPoint multiPoint = factory.createMultiPoint(new Point[] {
        factory.createPoint(new CoordinateXY(100, 20)),
        factory.createPoint(new CoordinateXY(100, 21)),
        factory.createPoint(new CoordinateXY(101, 21)),
    });

    GeometryCollection geometryCollection = factory.createGeometryCollection(new Geometry[] {
    new PointND(new GeometryFactory(), 2, 2.0, 12.0),
    multiLineString,
    polygon});
    geometries.add(geometryCollection);

    geometries.add(new EnvelopeND(new GeometryFactory(), 2, 0.0, 1.0, 25.0, 13.0));
    geometries.add(multiPoint);

    Configuration conf = sparkContext().hadoopConfiguration();
    Path outPath = new Path(scratchPath(), "test");
    Path outputFileName = new Path(outPath, "test.geojson");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    GeoJSONFeatureWriter writer = new GeoJSONFeatureWriter();
    try {
      writer.initialize(outputFileName, conf);
      for (Geometry geom : geometries) {
        Feature f = new Feature(geom, new String[] {"name"}, null, new Object[] {"name-value"});
        writer.write(null, f);
      }
    } finally {
      try {
        writer.close(null);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Read them back
    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(outputFileName, conf);
    try {
      int iGeom = 0;
      for (IFeature feature : reader) {
        // Compare the area since the envelope gets transformed into a polygon
        assertEquals("Error with geometry #"+iGeom, geometries.get(iGeom).getArea(), feature.getGeometry().getArea());
        iGeom++;
      }
    } finally {
      reader.close();
    }
  }
}