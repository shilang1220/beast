package edu.ucr.cs.bdlab.beast.io.tiff;

import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class TiffReaderTest extends JavaSparkTest {

  public void testReadSmallFile() throws IOException {
    Path inputFile = new Path(scratchPath(), "simple.tif");
    copyResource("/simple.tif", new File(inputFile.toString()));
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    TiffReader reader = new TiffReader();
    try {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      Raster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(8, raster.getWidth());
      assertEquals(8, raster.getHeight());
      raster.readTileData(0);
      assertEquals(0, raster.getPixel(0, 0));
      assertEquals(255, raster.getPixel(1, 0));
      assertEquals(0, raster.getPixel(7, 0));
      assertEquals(195, raster.getPixel(1, 7));
    } finally {
      reader.close();
    }
  }

  public void testReadStrippedFile() throws IOException {
    Path inputFile = new Path(scratchPath(), "simple.tif");
    copyResource("/FRClouds.tif", new File(inputFile.toString()));
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    TiffReader reader = new TiffReader();
    try {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      Raster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(99, raster.getWidth());
      assertEquals(72, raster.getHeight());
      raster.readTileData(0);
      assertEquals(0xeaf2e7, raster.getPixel(0, 0));
      assertEquals(0x566733, raster.getPixel(38, 31));
      assertEquals(0x23225e, raster.getPixel(76, 62));

      int[] components = new int[raster.getNumSamples()];
      raster.getPixelSamplesAsInt(76, 62, components);
      assertArrayEquals(new int[] {0x23, 0x22, 0x5e}, components);
    } finally {
      reader.close();
    }
  }

  public void testReadDeflateCompression() throws IOException {
    Path inputFile = new Path(scratchPath(), "simple.tif");
    copyResource("/FRClouds_Deflate.tif", new File(inputFile.toString()));
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    TiffReader reader = new TiffReader();
    try {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      Raster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(99, raster.getWidth());
      assertEquals(72, raster.getHeight());
      raster.readTileData(0);
      assertEquals(0xeaf2e7, raster.getPixel(0, 0));
      assertEquals(0x566733, raster.getPixel(38, 31));
      assertEquals(0x23225e, raster.getPixel(76, 62));

      int[] components = new int[raster.getNumSamples()];
      raster.getPixelSamplesAsInt(76, 62, components);
      assertArrayEquals(new int[] {0x23, 0x22, 0x5e}, components);
    } finally {
      reader.close();
    }
  }


  public void testRead16BitsInterleaved() throws IOException {
    Path inputFile = new Path(scratchPath(), "simple.tif");
    copyResource("/FRClouds_16bits.tif", new File(inputFile.toString()));
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    TiffReader reader = new TiffReader();
    try {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      Raster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(99, raster.getWidth());
      assertEquals(72, raster.getHeight());
      raster.readTileData(0);
      assertEquals(0xea00f200e7L, raster.getPixel(0, 0));
      assertEquals(0xe800ee00eaL, raster.getPixel(1, 0));
      assertEquals(0x5600670033L, raster.getPixel(38, 31));
      assertEquals(0x230022005eL, raster.getPixel(76, 62));

      int[] components = new int[raster.getNumSamples()];
      raster.getPixelSamplesAsInt(76, 62, components);
      assertArrayEquals(new int[] {0x23, 0x22, 0x5e}, components);
    } finally {
      reader.close();
    }
  }

  public void testReadBigEndianFile() throws IOException {
    Path inputFile = new Path(scratchPath(), "test.tif");
    copyResource("/glc2000_bigendian.tif", new File(inputFile.toString()));
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    TiffReader reader = new TiffReader();
    try {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      Raster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(256, raster.getWidth());
      assertEquals(128, raster.getHeight());
      // Blue pixel (ocean)
      assertEquals(0x8ae3ff, raster.getPixel(0, 0));
      assertEquals(0x8ae3ff, raster.getPixel(1, 0));
      assertEquals(0x8ae3ff, raster.getPixel(1, 1));
      assertEquals(0x8ae3ff, raster.getPixel(54, 24));
      // Red pixel (desert)
      assertEquals(0xff0000, raster.getPixel(137, 50));
    } finally {
      reader.close();
    }
  }

  public void testReadGriddedFile() throws IOException {
    Path inputFile = new Path(scratchPath(), "test.tif");
    copyResource("/glc2000_small.tif", new File(inputFile.toString()));
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    TiffReader reader = new TiffReader();
    try {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      Raster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(256, raster.getWidth());
      assertEquals(128, raster.getHeight());
      // Blue pixel (ocean)
      assertEquals(20, raster.getPixel(54, 24));
      // Gray pixel (desert)
      assertEquals(19, raster.getPixel(137, 58));
    } finally {
      reader.close();
    }
  }

  public void testReadBandedFile() throws IOException {
    Path inputFile = new Path(scratchPath(), "test.tif");
    copyResource("/glc2000_banded_small.tif", new File(inputFile.toString()));
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    TiffReader reader = new TiffReader();
    try {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      Raster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(256, raster.getWidth());
      assertEquals(128, raster.getHeight());
      // Read value from the first band
      assertEquals(896032.625f, raster.getSampleValueAsFloat(200, 100, 0));
      // Read value from the second band
      assertEquals(20, raster.getSampleValueAsInt(200, 100, 1));
    } finally {
      reader.close();
    }
  }
}