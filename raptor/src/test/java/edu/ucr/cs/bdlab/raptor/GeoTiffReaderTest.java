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

import edu.ucr.cs.bdlab.raptor.GeoTiffReader;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;

public class GeoTiffReaderTest extends JavaSparkTest {

  public void testStrippedGeoTIFF() throws IOException {
    Path file = new Path(scratchPath(), "test.tif");
    copyResource("/rasters/FRClouds.tif", new File(file.toString()));
    FileSystem fileSystem = file.getFileSystem(new Configuration());
    GeoTiffReader reader = new GeoTiffReader();
    try {
      reader.initialize(fileSystem, file, 0);

      assertEquals(99, reader.getRasterWidth());
      assertEquals(72, reader.getRasterHeight());
      assertEquals(0.17578125, reader.getPixelScaleX());
      // Transform origin point from raster to vector
      Point2D.Double outPoint = new Point2D.Double();
      reader.gridToModel(0, 0, outPoint);
      assertEquals(-6.679688 + 0.17578125 / 2, outPoint.getX(), 1E-3);
      assertEquals(53.613281 - 0.17578125 / 2, outPoint.getY(), 1E-3);
      // Transform the other corner point
      reader.gridToModel(reader.getRasterWidth(), reader.getRasterHeight(), outPoint);
      assertEquals(10.7226 + 0.17578125 / 2, outPoint.getX(), 1E-3);
      assertEquals(40.957 - 0.17578125 / 2, outPoint.getY(), 1E-3);
      // Test the inverse transformation on one corner
      reader.modelToGrid(-6.679688, 53.613281, outPoint);
      assertEquals(0, (int) outPoint.getX());
      assertEquals(0, (int) outPoint.getY());
      // Test the inverse transformation
      reader.modelToGrid(-0.06, 49.28, outPoint);
      assertEquals(37, (int) outPoint.getX());
      assertEquals(24, (int) outPoint.getY());
      int[] pixel = new int[reader.getNumComponents()];
      reader.getPointValueAsInt(-0.06, 49.28, pixel);
      assertArrayEquals(new int[]{69, 156, 139}, pixel);
    } finally {
      reader.close();
    }
  }

  public void testTiledGeoTIFF() throws IOException {
    Path file = new Path(scratchPath(), "test.tif");
    copyResource("/rasters/glc2000_small.tif", new File(file.toString()));
    FileSystem fileSystem = file.getFileSystem(new Configuration());
    GeoTiffReader reader = new GeoTiffReader();
    try {
      reader.initialize(fileSystem, file, 0);

      assertEquals(256, reader.getRasterWidth());
      assertEquals(128, reader.getRasterHeight());
      int[] pixel = new int[reader.getNumComponents()];
      reader.getPointValueAsInt(23.224, 32.415, pixel);
      assertArrayEquals(new int[]{8}, pixel);
      reader.getPointValueAsInt(33.694, 14.761, pixel);
      assertArrayEquals(new int[]{22}, pixel);
    } finally {
      reader.close();
    }
  }

  public void testBandedGeoTIFF() throws IOException {
    Path file = new Path(scratchPath(), "test.tif");
    copyResource("/rasters/glc2000_banded_small.tif", new File(file.toString()));
    FileSystem fileSystem = file.getFileSystem(new Configuration());
    GeoTiffReader reader = new GeoTiffReader();
    try {
      reader.initialize(fileSystem, file, 0);

      assertEquals(256, reader.getRasterWidth());
      assertEquals(128, reader.getRasterHeight());
      int[] pixelValueInt = new int[reader.getNumComponents()];
      reader.getPointValueAsInt(31.277, 26.954, pixelValueInt);
      assertArrayEquals(new int[]{880665, 16}, pixelValueInt);
      float[] pixelValueFloat = new float[reader.getNumComponents()];
      reader.getPointValueAsFloat(31.277, 26.954, pixelValueFloat);
      assertArrayEquals(new float[]{880664.8f, 16.0f}, pixelValueFloat, 1E-3F);
    } finally {
      reader.close();
    }
  }

  public void testProjectedGeoTIFF() throws IOException, FactoryException {
    Path file = new Path(scratchPath(), "test.tif");
    copyResource("/rasters/glc2000_small_EPSG3857.tif", new File(file.toString()));
    FileSystem fileSystem = file.getFileSystem(new Configuration());
    GeoTiffReader reader = new GeoTiffReader();
    try {
      reader.initialize(fileSystem, file, 0);

      assertEquals(5, reader.getRasterWidth());
      assertEquals(4, reader.getRasterHeight());
      assertEquals(CRS.decode("EPSG:3857", true), reader.getCRS());
    } finally {
      reader.close();
    }
  }

  public void testProjectedGeoTIFFLandsat() throws IOException, FactoryException {
    Path file = new Path(scratchPath(), "test.tif");
    copyResource("/rasters/sample.tif", new File(file.toString()));
    FileSystem fileSystem = file.getFileSystem(new Configuration());
    GeoTiffReader reader = new GeoTiffReader();
    ClassLoader classLoader = getClass().getClassLoader();
    File fileGeoTools = new File(scratchPath()+"/test.tif");
    try {
      reader.initialize(fileSystem, file, 0);

      CoordinateReferenceSystem crs = reader.getCRS();

      //GeoTools
      AbstractGridFormat format = GridFormatFinder.findFormat( fileGeoTools );
      GridCoverage2DReader Greader = format.getReader( fileGeoTools );
      GridCoverage2D coverage = (GridCoverage2D) Greader.read(null);
      CoordinateReferenceSystem crsGeotools = coverage.getCoordinateReferenceSystem();

      assertEquals(crsGeotools, crs);
    } finally {
      reader.close();
    }
  }
}