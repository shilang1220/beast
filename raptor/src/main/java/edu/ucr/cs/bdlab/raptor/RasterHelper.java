package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class RasterHelper {
    public static RasterReader createRasterReader(FileSystem rasterFS, Path rasterFile, Configuration conf) throws IOException {
      String extension = IOUtil.getExtension(rasterFile.getName());
      switch (extension.toLowerCase()) {
        case ".tif":
        case ".geotiff":
          GeoTiffReader greader = new GeoTiffReader();
          int layer = conf.getInt(RasterReader.RasterLayerID, 0);
          greader.initialize(rasterFS, rasterFile, layer);
          return greader;
        case ".hdf":
          HDF4Reader hreader = new HDF4Reader();
          String layerName = conf.get(RasterReader.RasterLayerID);
          hreader.initialize(rasterFS, rasterFile, layerName);
          return hreader;
        default:
          throw new RuntimeException(String.format("Unrecognized extension '%s'", extension));
      }
    }
}
