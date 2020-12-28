package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.raptor.GeoTiffReader;
import edu.ucr.cs.bdlab.raptor.HDF4Reader;
import edu.ucr.cs.bdlab.raptor.RasterHelper;
import edu.ucr.cs.bdlab.raptor.RasterReader;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class RasterReaderTest extends JavaSparkTest {

  public void testFactoryMethodGeoTIFF() throws IOException {
    Configuration conf = new Configuration();
    Path rasterFile = new Path(scratchPath(), "temp.tif");
    copyResource("/rasters/glc2000_small.tif", new File(rasterFile.toString()));
    FileSystem fs = rasterFile.getFileSystem(new Configuration());
    try (RasterReader reader = RasterHelper.createRasterReader(fs, rasterFile, conf)) {
      assertEquals(GeoTiffReader.class, reader.getClass());
      assertEquals(256, reader.getRasterWidth());
      assertEquals(128, reader.getRasterHeight());
    }
  }

  public void testFactoryMethodHDF4() throws IOException {
    Configuration conf = new Configuration();
    conf.set(RasterReader.RasterLayerID, "water_mask");
    Path rasterFile = new Path(scratchPath(), "test.hdf");
    copyResource("/rasters/MOD44W.A2000055.h07v06.005.2009212172956.hdf", new File(rasterFile.toString()));
    FileSystem fs = rasterFile.getFileSystem(new Configuration());
    try (RasterReader reader = RasterHelper.createRasterReader(fs, rasterFile, conf)) {
      assertEquals(HDF4Reader.class, reader.getClass());
      assertEquals(4800, reader.getRasterWidth());
      assertEquals(4800, reader.getRasterHeight());
    }
  }
}