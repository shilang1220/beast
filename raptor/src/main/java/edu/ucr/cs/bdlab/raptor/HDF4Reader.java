package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.jhdf.DDNumericDataGroup;
import edu.ucr.cs.bdlab.jhdf.DDVDataHeader;
import edu.ucr.cs.bdlab.jhdf.DDVGroup;
import edu.ucr.cs.bdlab.jhdf.DataDescriptor;
import edu.ucr.cs.bdlab.jhdf.HDFConstants;
import edu.ucr.cs.bdlab.jhdf.HDFFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.crs.DefaultProjectedCRS;
import org.geotools.referencing.cs.DefaultCartesianCS;
import org.geotools.referencing.cs.DefaultEllipsoidalCS;
import org.geotools.referencing.datum.DefaultEllipsoid;
import org.geotools.referencing.datum.DefaultGeodeticDatum;
import org.geotools.referencing.datum.DefaultPrimeMeridian;
import org.geotools.referencing.operation.DefaultMathTransformFactory;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.awt.*;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads HDF4 files.
 */
public class HDF4Reader implements RasterReader, Closeable {

  /**Boundaries of the Sinusoidal space assuming angles in radians and multiplying by the scale factor 6371007.181*/
  protected static final double Scale = 6371007.181;
  protected static final double MinX = -Math.PI * Scale;
  protected static final double MaxX = +Math.PI * Scale;
  protected static final double MinY = -Math.PI / 2.0 * Scale;
  protected static final double MaxY = +Math.PI / 2.0 * Scale;
  protected static final double TileWidth = (MaxX - MinX) / 36;
  protected static final double TileHeight = (MaxY - MinY) / 18;

  /**The coordinate reference system for the sinusoidal space*/
  public static final CoordinateReferenceSystem SinusoidalCRS;

  static {
    try {
      // https://spatialreference.org/ref/sr-org/modis-sinusoidal/
      SinusoidalCRS = new DefaultProjectedCRS("Sinusoidal",
          new DefaultGeographicCRS(
              new DefaultGeodeticDatum("World", DefaultEllipsoid.WGS84, DefaultPrimeMeridian.GREENWICH),
              DefaultEllipsoidalCS.GEODETIC_2D
          ),
          //sinus.getConversionFromBase.getMathTransform,
          new DefaultMathTransformFactory().createFromWKT("PARAM_MT[\"Sinusoidal\", \n  PARAMETER[\"semi_major\", 6371007.181], \n  PARAMETER[\"semi_minor\", 6371007.181], \n  PARAMETER[\"central_meridian\", 0.0], \n  PARAMETER[\"false_easting\", 0.0], \n  PARAMETER[\"false_northing\", 0.0]]"),
          DefaultCartesianCS.PROJECTED
      );
    } catch (FactoryException e) {
      throw new RuntimeException("Could not create Sinusoidal CRS");
    }
  }

  /**The underlying HDF file*/
  protected HDFFile hdfFile;

  /**The ID of the tile in the MODIS grid*/
  protected int h, v;

  /**The range of valid values from the header of the HDF file*/
  protected int minValue, maxValue;

  /**The name of the layer to read from the HDF file*/
  protected String layerName;

  /**The grid size of the raster file*/
  protected int resolution;

  /**The buffer that contains all the raster data (dcompressed and non-parsed)*/
  protected ByteBuffer dataBuffer;

  /**The size of one value entry in bytes*/
  protected int valueSize;

  /**The fill value as listed in the header of the file*/
  protected int fillValue;

  /**The scale factor listed in the header or 1.0 if not listed in the HDF file*/
  protected double scaleFactor;

  public HDF4Reader() {
  }

  /**
   * Initializes the reader to read the given file and choose the given layer out of it.
   * @param fs the file system that contains the file
   * @param path the path to the file
   * @param layerName the name of the layer to open in the HDF file
   * @throws IOException if an error happens while loading the file
   */
  public void initialize(FileSystem fs, Path path, String layerName) throws IOException {
    if (this.hdfFile != null) {
      // In case the reader is reused without properly closing it
      this.close();
    }
    this.hdfFile = new HDFFile(path.toString());
    // Retrieve the h and v values (only for MODIS files from the LP DAAC archive)
    String archiveMetadata = (String) hdfFile.findHeaderByName("ArchiveMetadata.0").getEntryAt(0);
    String coreMetadata = (String) hdfFile.findHeaderByName("CoreMetadata.0").getEntryAt(0);
    try {
      this.h = getIntByName(archiveMetadata, "HORIZONTALTILENUMBER");
      this.v = getIntByName(archiveMetadata, "VERTICALTILENUMBER");
    } catch (RuntimeException e) {
      // For WaterMask (MOD44W), these values are found somewhere else
      try {
        this.h = getIntByName(coreMetadata, "HORIZONTALTILENUMBER");
        this.v = getIntByName(coreMetadata, "VERTICALTILENUMBER");
      } catch (RuntimeException e2) {
        throw new RuntimeException("Could not getPixel h and v values for an HDF file");
      }
    }
    // DATACOLUMNS is not available in MODIS temperature product MOD11A1.006
    //this.resolution = getIntByName(archiveMetadata, "DATACOLUMNS");
    this.layerName = layerName;

    // TODO lazily load the raster when a pixel is requested
    this.loadRaster(null);
  }

  private static int getIntByName(String metadata, String name) {
    String strValue = getStringByName(metadata, name);
    if (strValue == null)
      throw new RuntimeException(String.format("Couldn't find value with name '%s'", name));
    return Integer.parseInt(strValue);
  }

  private static String getStringByName(String metadata, String name) {
    int offset = metadata.indexOf(name);
    if (offset == -1)
      return null;
    offset = metadata.indexOf(" VALUE", offset);
    if (offset == -1)
      return null;
    offset = metadata.indexOf('=', offset);
    if (offset == -1)
      return null;
    do offset++; while (offset < metadata.length() &&
        metadata.charAt(offset) == ' ' || metadata.charAt(offset) == '"');
    int endOffset = offset;
    do endOffset++;  while (endOffset < metadata.length() && metadata.charAt(endOffset) != ' '
        && metadata.charAt(endOffset) != '"'
        && metadata.charAt(endOffset) != '\n'
        && metadata.charAt(endOffset) != '\r');
    if (offset < metadata.length())
      return metadata.substring(offset, endOffset);
    return null;
  }


  public void loadRaster(Rectangle rect) throws IOException {
    if (this.dataBuffer != null)
      return;
    // TODO use the rect to read part of the raster file
    // Find the required dataset and read it
    DDVGroup dataGroup = hdfFile.findGroupByName(layerName);
    if (dataGroup == null)
      throw new RuntimeException(String.format("Could not find the layer '%s' in the HDF file", layerName));
    boolean fillValueFound = false;
    scaleFactor = 1.0f;
    for (DataDescriptor dd : dataGroup.getContents()) {
      if (dd instanceof DDVDataHeader) {
        DDVDataHeader vheader = (DDVDataHeader) dd;
        if (vheader.getName().equals("_FillValue")) {
          Object unparsedFillValue = vheader.getEntryAt(0);
          if (unparsedFillValue instanceof Integer)
            this.fillValue = (Integer) unparsedFillValue;
          else if (unparsedFillValue instanceof Short)
            this.fillValue = (Short) unparsedFillValue;
          else if (unparsedFillValue instanceof Byte)
            this.fillValue = (Byte) unparsedFillValue;
          else
            throw new RuntimeException("Unsupported type: "+unparsedFillValue.getClass());
          fillValueFound = true;
        } else if (vheader.getName().equals("valid_range")) {
          Object minValue = vheader.getEntryAt(0);
          if (minValue instanceof Integer)
            this.minValue = (Integer) minValue;
          else if (minValue instanceof Short)
            this.minValue = (Short) minValue;
          else if (minValue instanceof Byte)
            this.minValue = (Byte) minValue;
          Object maxValue = vheader.getEntryAt(1);
          if (maxValue instanceof Integer)
            this.maxValue = (Integer) maxValue;
          else if (maxValue instanceof Short)
            this.maxValue = (Short) maxValue;
          else if (maxValue instanceof Byte)
            this.maxValue = (Byte) maxValue;
        } else if (vheader.getName().equals("scale_factor")) {
          Object unparsedValue = vheader.getEntryAt(0);
          if (unparsedValue instanceof Integer)
            this.scaleFactor = (Integer) unparsedValue;
          else if (unparsedValue instanceof Short)
            this.scaleFactor = (Short) unparsedValue;
          else if (unparsedValue instanceof Byte)
            this.scaleFactor = (Byte) unparsedValue;
          else if (unparsedValue instanceof Float)
            this.scaleFactor = (Float) unparsedValue;
          else if (unparsedValue instanceof Double)
            this.scaleFactor = (Double) unparsedValue;
          else
            throw new RuntimeException("Unsupported type: "+unparsedValue.getClass());
          // Hack: The scale factor in NDVI data is listed as 10,000 but it should be 1/10,000
          // See: https://lpdaac.usgs.gov/products/mod13q1v006/
          if (scaleFactor == 10000.0)
            scaleFactor = 1/10000.0;
        }
      }
    }
    // Retrieve data
    for (DataDescriptor dd : dataGroup.getContents()) {
      if (dd instanceof DDNumericDataGroup) {
        DDNumericDataGroup numericDataGroup = (DDNumericDataGroup) dd;
        valueSize = numericDataGroup.getDataSize();
        resolution = numericDataGroup.getDimensions()[0];
        byte[] unparsedDataArray = new byte[valueSize * resolution * resolution];
        if (fillValueFound) {
          byte[] fillValueBytes = new byte[valueSize];
          HDFConstants.writeAt(fillValueBytes, 0, this.fillValue, valueSize);
          for (int i = 0; i < unparsedDataArray.length; i++)
            unparsedDataArray[i] = fillValueBytes[i % valueSize];
        }
        numericDataGroup.getAsByteArray(unparsedDataArray, 0, unparsedDataArray.length);
        this.dataBuffer = ByteBuffer.wrap(unparsedDataArray);
      }
    }
  }

  @Override
  public int getTileID(int iPixel, int jPixel) {
    return 0;
  }

  @Override
  public int getNumTiles() {
    return 1;
  }

  @Override
  public int getNumTilesX() {
    return 1;
  }

  @Override
  public int getNumTilesY() {
    return 1;
  }

  @Override
  public int getY1() {
    return 0;
  }

  @Override
  public int getY2() {
    return resolution;
  }

  @Override
  public int getX1() {
    return 0;
  }

  @Override
  public int getX2() {
    return resolution;
  }

  @Override
  public int getTileX1(int tileID) {
    return 0;
  }

  @Override
  public int getTileX2(int tileID) {
    return resolution;
  }

  @Override
  public int getRasterWidth() {
    return resolution;
  }

  @Override
  public int getRasterHeight() {
    return resolution;
  }

  /**
   * In the case of HDF4 files, the model is the scaled Sinusodial space and the grid is the matrix stored
   * in this file.
   * @param mx x-coordinate in the scaled Sinusoidal space
   * @param my y-coordinate in the scaled Sinusodial space
   * @param outPoint the point that contains the grid (pixel) coordinate
   */
  @Override
  public void modelToGrid(double mx, double my, Point2D.Double outPoint) {
    outPoint.x = (mx - MinX - h * TileWidth) * resolution / TileWidth;
    outPoint.y = -(my - MaxY + v * TileHeight) * resolution / TileHeight;
  }

  @Override
  public void gridToModel(int gx, int gy, Point2D.Double outPoint) {
    outPoint.x = MinX + h * TileWidth + (gx + 0.5) * TileWidth / resolution;
    outPoint.y = MaxY - v * TileHeight - (gy + 0.5) * TileHeight / resolution;
  }

  @Override
  public void getPixelValueAsInt(int iPixel, int jPixel, int[] value) throws IOException {
    assert value.length == 1 : "Mulitband HDF files are not yet support";
    switch (valueSize) {
      case 1: value[0] = dataBuffer.get((jPixel * resolution + iPixel) * valueSize); break;
      case 2: value[0] = dataBuffer.getShort((jPixel * resolution + iPixel) * valueSize); break;
      case 4: value[0] = dataBuffer.getInt((jPixel * resolution + iPixel) * valueSize); break;
      default:
        throw new RuntimeException(String.format("Value size %d not yet supported", valueSize));
    }
  }

  @Override
  public boolean isFillValue(int iPixel, int jPixel) {
    switch (valueSize) {
      case 1: return dataBuffer.get((jPixel * resolution + iPixel) * valueSize) == fillValue;
      case 2: return dataBuffer.getShort((jPixel * resolution + iPixel) * valueSize) == fillValue;
      case 4: return dataBuffer.getInt((jPixel * resolution + iPixel) * valueSize) == fillValue;
      default:
        throw new RuntimeException(String.format("Value size %d not yet supported", valueSize));
    }
  }

  @Override
  public void getPixelValueAsFloat(int iPixel, int jPixel, float[] value) throws IOException {
    int[] intvalue = new int[value.length];
    getPixelValueAsInt(iPixel, jPixel, intvalue);
    for (int i = 0; i < intvalue.length; i++)
      value[i] = (float) (intvalue[i] * scaleFactor);
  }

  @Override
  public int getFillValue() {
    return fillValue;
  }

  @Override
  public int getNumComponents() {
    return 1;
  }

  @Override
  public CoordinateReferenceSystem getCRS() {
    return SinusoidalCRS;
  }

  @Override
  public void close() throws IOException {
    this.dataBuffer = null;
    hdfFile.close();
  }

  /**Format of file names in MODIS repository with tile identifier in the name*/
  public static final Pattern ModisTileIDRegex = Pattern.compile(".*h(\\d\\d)v(\\d\\d).*");

  /**
   * Create a path filter that selects only the tiles that match the given rectangle in the Sinusoidal space.
   * @param rect the extents of the range to compute the filter for in the Sinusoidal space
   * @return a Path filter that will match the tiles based on the file name using the <tt>hxxvyy</tt> part
   */
  public static PathFilter createTileIDFilter(Rectangle2D rect) {
    int hMin = (int) ((rect.getX() - MinX) / TileWidth);
    int hMax = (int) ((rect.getMaxX() - MinX) / TileWidth);
    int vMax = (int) ((-rect.getY() - MinY) / TileHeight);
    int vMin = (int) ((-rect.getMaxY() - MinY) / TileHeight);
    Rectangle tileRange = new Rectangle(hMin, vMin, hMax - hMin + 1, vMax - vMin + 1);
    return p -> {
      Matcher matcher = ModisTileIDRegex.matcher(p.getName());
      if (!matcher.matches())
        return false;
      int h = Integer.parseInt(matcher.group(1));
      int v = Integer.parseInt(matcher.group(2));
      return tileRange.contains(h, v);
    };
  }

  /**Format of directory names in MODIS with embedded date*/
  public static final SimpleDateFormat DateFormat = new SimpleDateFormat("yyyy.MM.dd");

  /**
   * Creates a filter for paths that match the given range of dates inclusive of both start and end dates.
   * Each date is in the format "yyyy.mm.dd".
   * @param dateStart the start date as a string in the "yyyy.mm.dd" format (inclusive)
   * @param dateEnd the end date (inclusive)
   * @return a PathFilter that will match all dates in the given range
   */
  public static PathFilter createDateFilter(String dateStart, String dateEnd) {
    try {
      final long startTime = DateFormat.parse(dateStart).getTime();
      final long endTime = DateFormat.parse(dateEnd).getTime();
      return p -> {
        try {
          long time = DateFormat.parse(p.getName()).getTime();
          return time >= startTime && time <= endTime;
        } catch (ParseException e) {
          return false;
        }
      };
    } catch (ParseException e) {
      e.printStackTrace();
      return p -> true;
    }
  }
}
