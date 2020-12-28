package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.raptor.HDF4Reader;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;

public class HDF4ReaderTest extends JavaSparkTest {

  public void testMetadata() throws IOException {
    Path hdfFile = new Path(scratchPath(), "test.hdf");
    copyResource("/rasters/MOD44W.A2000055.h07v06.005.2009212172956.hdf", new File(hdfFile.toString()));
    FileSystem fileSystem = hdfFile.getFileSystem(new Configuration());
    try (HDF4Reader reader = new HDF4Reader()) {
      reader.initialize(fileSystem, hdfFile, "water_mask");
      assertEquals(1, reader.getNumComponents());
      assertEquals(4800, reader.getRasterWidth());
      int[] value = new int[reader.getNumComponents()];
      reader.getPixelValueAsInt(0, 0, value);
      assertEquals(1, value[0]);
      assertEquals(1, reader.getNumTiles());
    }
  }

  public void testProjection() throws IOException {
    Path hdfFile = new Path(scratchPath(), "test.hdf");
    copyResource("/rasters/MOD44W.A2000055.h07v06.005.2009212172956.hdf", new File(hdfFile.toString()));
    FileSystem fileSystem = hdfFile.getFileSystem(new Configuration());
    try (HDF4Reader reader = new HDF4Reader()) {
      reader.initialize(fileSystem, hdfFile, "water_mask");
      Point2D.Double pt = new Point2D.Double();
      reader.gridToModel(0, 0, pt);
      assertEquals(Math.toRadians(-110 + 10 * 0.5 / 4800) * HDF4Reader.Scale, pt.x, 1E-3);
      assertEquals(Math.toRadians(30.0 - 10 * 0.5 / 4800) * HDF4Reader.Scale, pt.y, 1E-3);

      reader.modelToGrid(Math.toRadians(-110.0) * HDF4Reader.Scale, Math.toRadians(30.0) * HDF4Reader.Scale, pt);
      assertEquals(0.0, pt.x, 1E-3);
      assertEquals(0.0, pt.y, 1E-3);

      reader.modelToGrid(Math.toRadians(-110.0 + 10.0 / 4800) * HDF4Reader.Scale, Math.toRadians(30.0 - 10.0 / 4800) * HDF4Reader.Scale, pt);
      assertEquals(1.0, pt.x, 1E-3);
      assertEquals(1.0, pt.y, 1E-3);

      reader.gridToModel(50, 122, pt);
      reader.modelToGrid(pt.x, pt.y, pt);
      assertEquals(50.5, pt.x, 1E-3);
      assertEquals(122.5, pt.y, 1E-3);

      reader.gridToModel(4799, 4799, pt);
      assertEquals(Math.toRadians(-100 - 10 * 0.5 / 4800)* HDF4Reader.Scale, pt.x, 1E-3);
      assertEquals(Math.toRadians(20.0 + 10 * 0.5 / 4800)* HDF4Reader.Scale, pt.y, 1E-3);

    }
  }

  public void testSelectTiles() {
    PathFilter tileIDFilter = HDF4Reader.createTileIDFilter(new Rectangle2D.Double(
            Math.toRadians(-145.0) * HDF4Reader.Scale,
            Math.toRadians(5.0) * HDF4Reader.Scale,
            Math.toRadians(29.0) * HDF4Reader.Scale,
            Math.toRadians(49.0) * HDF4Reader.Scale));
    assertTrue(tileIDFilter.accept(new Path("tile-h03v03.hdf")));
    assertTrue(tileIDFilter.accept(new Path("tile-h06v07.hdf")));
    assertFalse(tileIDFilter.accept(new Path("tile-h02v09.hdf")));
    assertFalse(tileIDFilter.accept(new Path("tile-h07v06.hdf")));
  }

  public void testSelectDates() {
    PathFilter dateFilter = HDF4Reader.createDateFilter("2001.02.15", "2005.02.11");
    assertTrue(dateFilter.accept(new Path("2001.02.15")));
    assertTrue(dateFilter.accept(new Path("2005.02.11")));
    assertTrue(dateFilter.accept(new Path("2003.07.15")));
    assertFalse(dateFilter.accept(new Path("2005.02.12")));
    assertFalse(dateFilter.accept(new Path("2001.01.31")));
  }

  public void testLoadAnotherFile() throws IOException {
    Path hdfFile = new Path(scratchPath(), "test.hdf");
    copyResource("/rasters/MOD44W.A2000055.h07v06.005.2009212172956.hdf", new File(hdfFile.toString()));
    FileSystem fileSystem = hdfFile.getFileSystem(new Configuration());
    HDF4Reader reader = new HDF4Reader();
    reader.initialize(fileSystem, hdfFile, "water_mask");
    int[] value = new int[reader.getNumComponents()];
    reader.getPixelValueAsInt(0, 0, value);
    assertEquals(1, value[0]);
    reader.close();

    // Load a different file
    hdfFile = new Path(scratchPath(), "test2.hdf");
    copyResource("/rasters/MYD11A1.A2002185.h09v06.006.2015146150958.hdf", new File(hdfFile.toString()));
    reader.initialize(fileSystem, hdfFile, "LST_Day_1km");
    value = new int[reader.getNumComponents()];
    reader.getPixelValueAsInt(0, 0, value);
    assertEquals(15240, value[0]);
    reader.close();

  }
}