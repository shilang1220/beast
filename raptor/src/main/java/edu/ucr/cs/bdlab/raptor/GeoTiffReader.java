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

import edu.ucr.cs.bdlab.beast.io.tiff.AbstractIFDEntry;
import edu.ucr.cs.bdlab.beast.io.tiff.ITiffReader;
import edu.ucr.cs.bdlab.beast.io.tiff.Raster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.geom.Point2D;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class GeoTiffReader implements RasterReader, Closeable {

  private static final Log LOGGER = LogFactory.getLog(GeoTiffReader.class);
  /**
   * Type = DOUBLE (IEEE Double precision)
   * N = 3
   * This tag may be used to specify the size of raster pixel spacing in the
   * model space units, when the raster space can be embedded in the model
   * space coordinate system without rotation, and consists of the following 3 values:
   * ModelPixelScaleTag = (ScaleX, ScaleY, ScaleZ)
   * where ScaleX and ScaleY give the horizontal and vertical spacing of
   * raster pixels. The ScaleZ is primarily used to map the pixel value of a
   * digital elevation model into the correct Z-scale, and so for most other
   * purposes this value should be zero (since most model spaces are 2-D, with Z=0).
   * For more details, check the GeoTIFF specs page 26.
   */
  public static final short ModelPixelScaleTag = (short) 33550;

  /**
   * Type = DOUBLE (IEEE Double precision)
   * N = 6*K, K = number of tiepoints
   * Alias: GeoreferenceTag
   * This tag stores raster-&gt;model tiepoint pairs in the order
   * ModelTiepointTag = (...,I,J,K, X,Y,Z...),
   */
  public static final short ModelTiepointTag = (short) 33922;

  /**
   * This tag may be used to store the GeoKey Directory, which defines and references
   * the "GeoKeys", as described below.
   * The tag is an array of unsigned SHORT values, which are primarily grouped into
   * blocks of 4. The first 4 values are special, and contain GeoKey directory header
   * information. The header values consist of the following information, in order:
   * Header={KeyDirectoryVersion, KeyRevision, MinorRevision, NumberOfKeys}
   * where
   * "KeyDirectoryVersion" indicates the current version of Key
   * implementation, and will only change if this Tag's Key
   * structure is changed. (Similar to the TIFFVersion (42)).
   * The current DirectoryVersion number is 1. This value will
   * most likely never change, and may be used to ensure that
   * this is a valid Key-implementation.
   * "KeyRevision" indicates what revision of Key-Sets are used.
   * "MinorRevision" indicates what set of Key-codes are used. The
   * complete revision number is denoted &lt;KeyRevision&gt;.&lt;MinorRevision&gt;
   * "NumberOfKeys" indicates how many Keys are defined by the rest
   * of this Tag.
   * This header is immediately followed by a collection of &lt;NumberOfKeys&gt; KeyEntry
   * sets, each of which is also 4-SHORTS long. Each KeyEntry is modeled on the
   * "TIFFEntry" format of the TIFF directory header, and is of the form:
   * KeyEntry = { KeyID, TIFFTagLocation, Count, Value_Offset }
   * where
   * "KeyID" gives the key-ID value of the Key (identical in function
   * to TIFF tag ID, but completely independent of TIFF tag-space),
   * "TIFFTagLocation" indicates which TIFF tag contains the value(s)
   * of the Key: if TIFFTagLocation is 0, then the value is SHORT,
   * and is contained in the "Value_Offset" entry. Otherwise, the type
   * (format) of the value is implied by the TIFF-Type of the tag
   * containing the value.
   * "Count" indicates the number of values in this key.
   * "Value_Offset" Value_Offset indicates the index-
   * offset *into* the TagArray indicated by TIFFTagLocation, if
   * it is nonzero. If TIFFTagLocation=0, then Value_Offset
   * contains the actual (SHORT) value of the Key, and
   * Count=1 is implied. Note that the offset is not a byte-offset,
   * but rather an index based on the natural data type of the
   * specified tag array.
   * Following the KeyEntry definitions, the KeyDirectory tag may also contain
   * additional values. For example, if a Key requires multiple SHORT values, they
   * shall be placed at the end of this tag, and the KeyEntry will set
   * TIFFTagLocation=GeoKeyDirectoryTag, with the Value_Offset pointing to the
   * location of the value(s).
   * All key-values which are not of type SHORT are to be stored in one of the following
   * two tags, based on their format:
   */
  public static final short GeoKeyDirectoryTag = (short) 34735;

  /**
   * This tag is used to store all of the ASCII valued GeoKeys, referenced by the
   * GeoKeyDirectoryTag. Since keys use offsets into tags, any special comments
   * may be placed at the beginning of this tag. For the most part, the only keys
   * that are ASCII valued are "Citation" keys, giving documentation and references
   * for obscure projections, datums, etc.
   * Note on ASCII Keys:
   * Special handling is required for ASCII-valued keys. While it is true that TIFF
   * 6.0 permits multiple NULL-delimited strings within a single ASCII tag, the
   * secondary strings might not appear in the output of naive "tiffdump" programs.
   * For this reason, the null delimiter of each ASCII Key value shall be converted
   * to a "|" (pipe) character before being installed back into the ASCII holding
   * tag, so that a dump of the tag will look like this.
   * AsciiTag="first_value|second_value|etc...last_value|"
   * A baseline GeoTIFF-reader must check for and convert the final "|" pipe character
   * of a key back into a NULL before returning it to the client software.
   * In the TIFF spec it is required that TIFF tags be written out to the file in
   * tag-ID sorted order. This is done to avoid forcing software to perform N-squared
   * sort operations when reading and writing tags.
   * To follow the TIFF philosophy, GeoTIFF-writers shall store the GeoKey entries
   * in key-sorted order within the CoordSystemInfoTag.
   * Example:
   * GeoKeyDirectoryTag=( 1, 1, 2, 6,
   *                   1024, 0, 1, 2,
   *                   1026, 34737,12, 0,
   *                   2048, 0, 1, 32767,
   *                   2049, 34737,14, 12,
   *                   2050, 0, 1, 6,
   *                   2051, 34736, 1, 0 )
   * GeoDoubleParamsTag(34736)=(1.5)
   * GeoAsciiParamsTag(34737)=("Custom File|My Geographic|")
   * The first line indicates that this is a Version 1 GeoTIFF GeoKey directory,
   * the keys are Rev. 1.2, and there are 6 Keys defined in this tag.
   * The next line indicates that the first Key (ID=1024 = GTModelTypeGeoKey) has
   * the value 2 (Geographic), explicitly placed in the entry list (since
   * TIFFTagLocation=0). The next line indicates that the Key 1026 (the
   * GTCitationGeoKey) is listed in the GeoAsciiParamsTag (34737) array, starting
   * at offset 0 (the first in array), and running for 12 bytes and so has the value
   * "Custom File" (the "|" is converted to a null delimiter at the end). Going
   * further down the list, the Key 2051 (GeogLinearUnitSizeGeoKey) is located in
   * the GeoDoubleParamsTag (34736), at offset 0 and has the value 1.5; the value
   * of key 2049 (GeogCitationGeoKey) is "My Geographic".
   * The TIFF layer handles all the problems of data structure, platform independence,
   * format types, etc, by specifying byte-offsets, byte-order format and count,
   * while the Key describes its key values at the TIFF level by specifying Tag
   * number, array-index, and count. Since all TIFF information occurs in TIFF arrays
   * of some sort, we have a robust method for storing anything in a Key that would
   * occur in a Tag.
   * With this Key-value approach, there are 65536 Keys which have all the flexibility
   * of TIFF tag, with the added advantage that a TIFF dump will provide all the
   * information that exists in the GeoTIFF implementation.
   * This GeoKey mechanism will be used extensively in section 2.7, where the numerous
   * parameters for defining Coordinate Systems and their underlying projections
   * are defined.
   */
  public static final short GeoAsciiParamsTag = (short) 34737;

  /**
   * Used by the GDAL library, holds an XML list of name=value 'metadata' values about the image as a whole,
   * and about specific samples.
   */
  public static final short GDALMetadataTag = (short) 42112;

  /**
   * Used by the GDAL library, contains an ASCII encoded nodata or background pixel value.
   */
  public static final short GDALNoDataTag = (short) 42113	;

  public static short rasterCRS;

  public static int Fillvalue;

  public CoordinateReferenceSystem crs;

  static {
    Raster.TagNames.put(ModelPixelScaleTag, "Model Pixel Scale");
    Raster.TagNames.put(ModelTiepointTag, "Model Tiepoint");
    Raster.TagNames.put(GeoKeyDirectoryTag, "Geokey Directory");
    Raster.TagNames.put(GeoAsciiParamsTag, "Geo ASCII Parameters");
    Raster.TagNames.put(GDALMetadataTag, "GDAL Metadata");
    Raster.TagNames.put(GDALNoDataTag, "GDAL NoData");
  }

  /** Reader for the base TIFF file */
  protected ITiffReader reader;

  /** The desired raster layer from the TIFF file*/
  protected Raster raster;

  /**Grid to Model transformation matrix*/
  protected AffineTransform g2m;

  public GeoTiffReader() {}

  /**
   * Loads the first layer in the given GeoTIFF file
   * @param fileSystem the file system that contains the GeoTIFF file
   * @param path the path to the file
   * @throws IOException if an error happens while reading the file
   */
  public void initialize(FileSystem fileSystem, Path path) throws IOException {
    this.initialize(fileSystem, path, 0);
  }

  /**
   * Loads one layer from a GeoTIFF file.
   * @param fileSystem the file system that contains the GeoTIFF file
   * @param path the path to the file
   * @param iLayer the index of the layer to reader from the file
   * @throws IOException if an error happens while reading the file
   */
  public void initialize(FileSystem fileSystem, Path path, int iLayer) throws IOException {
    // Loads the GeoTIFF file from the given path
    reader = ITiffReader.openFile(fileSystem.open(path));
    raster = reader.getLayer(iLayer);
    //raster.dumpEntries(System.out);
    // Define grid to model transformation (G2M)
    ByteBuffer buffer = null;
    this.g2m = new AffineTransform();
    AbstractIFDEntry entry = raster.getEntry(ModelTiepointTag);
    if (entry != null) {
      // Translate point
      buffer = reader.readEntry(entry, buffer);
      double dx = buffer.getDouble(3 * 8) - buffer.getDouble(0 * 8);
      double dy = buffer.getDouble(4 * 8) - buffer.getDouble(1 * 8);
      g2m.translate(dx, dy);
    }
    entry = raster.getEntry(ModelPixelScaleTag);
    if (entry != null) {
      // Scale point
      buffer = reader.readEntry(entry, null);
      double sx = buffer.getDouble(0 * 8);
      double sy = buffer.getDouble(1 * 8);
      g2m.scale(sx, -sy);
    }

    entry = raster.getEntry(GDALNoDataTag);
    if(entry!=null){
      buffer =reader.readEntry(entry,null);
      int prefix = Character.getNumericValue(buffer.get(0));
      if(prefix!= -1)
          Fillvalue = prefix;
      else
          Fillvalue = 0;
      for(int i = 1; i<buffer.capacity()-1; i++)
      {
          byte value = buffer.get(i);
          int temp = Character.getNumericValue(value);
          Fillvalue = Fillvalue*10 + temp;
      }
      if(prefix == -1)
          Fillvalue*= -1;


    }
    else
        Fillvalue = Integer.MIN_VALUE;

    // call to class to get metadata.
    GeoTiffMetadata metadata = new GeoTiffMetadata(reader,raster);
    GeoTiffMetadata2CRSAdapter gtcs = new GeoTiffMetadata2CRSAdapter(null);
    try {
       crs = gtcs.createCoordinateSystem(metadata);
    } catch (Exception e) {
      LOGGER.warn("Unable to parse GeoTiff CRS",e);
    }
  }

  @Override
  public int getTileID(int iPixel, int jPixel) {
    int iTile = iPixel / raster.getTileWidth();
    int jTile = jPixel / raster.getTileHeight();
    return jTile * getNumTilesX() + iTile;
  }

  @Override
  public int getNumTiles() {
    return raster.getNumTilesX() * raster.getNumTilesY();
  }

  @Override
  public int getNumTilesX() {
    return raster.getNumTilesX();
  }

  @Override
  public int getNumTilesY() {
    return raster.getNumTilesY();
  }

  @Override
  public int getY1() {
    return 0;
  }

  @Override
  public int getY2() {
    return raster.getHeight()-1;
  }

  @Override
  public int getX1() {
    return 0;
  }

  @Override
  public int getX2() {
    return raster.getWidth()-1;
  }

  @Override
  public int getTileX1(int tileID) {
    int iTile = tileID % getNumTilesX();
    return getX1() + iTile * raster.getTileWidth();
  }

  @Override
  public int getTileX2(int tileID) {
    int iTile = tileID % getNumTilesX();
    return getX1() + (iTile + 1) * raster.getTileWidth() - 1;
  }

  public int getTileWidth(){return  raster.getTileWidth();}
  public  int getTileHeight(){return  raster.getTileHeight();}
  @Override
  public int getRasterWidth() {
    return raster.getWidth();
  }

  @Override
  public int getRasterHeight() {
    return raster.getHeight();
  }

  @Override
  public void modelToGrid(double wx, double wy, Point2D.Double outPoint) {
    outPoint.setLocation(wx, wy);
    try {
      g2m.inverseTransform(outPoint, outPoint);
    } catch (NoninvertibleTransformException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void gridToModel(int gx, int gy, Point2D.Double outPoint) {
    outPoint.setLocation(gx + 0.5, gy + 0.5);
    g2m.transform(outPoint, outPoint);
  }

  @Override
  public boolean isFillValue(int iPixel, int jPixel) throws IOException {
    for (int i = 0; i < getNumComponents(); i++) {
      if (raster.getSampleValueAsInt(iPixel, jPixel, i) == Fillvalue)
        return true;
    }
    return false;
  }

  @Override
  public void getPixelValueAsInt(int iPixel, int jPixel, int[] value) throws IOException {
    raster.getPixelSamplesAsInt(iPixel, jPixel, value);
  }

  @Override
  public void getPixelValueAsFloat(int iPixel, int jPixel, float[] value) throws IOException {
    raster.getPixelSamplesAsFloat(iPixel, jPixel, value);
  }


  @Override
  public int getFillValue() {
    return Fillvalue;
  }

  @Override
  public int getNumComponents() {
    return raster.getNumSamples();
  }

  /*CRS definition of the file is held by key 34735:GeoKeyDirectory
  Key 1024: GTModelTypegeoKey
   ModelTypeProjected = 1 Projection Coordinate System
   ModelTypeGeographic = 2 Geographic latitude-longitude System
   ModelTypeGeocentric = 3 Geocentric (X,Y,Z) Coordinate System
   Key 1025 : GTRaterTypeGeoKey
   RasterPixelIsArea = 1
   RasterPixelIsPoint = 2
   Key 2054 : GeogAngularUnitsGeoKey
   Angular Units
   Angular_Radian = 9101
   Angular_Degree = 9102
   Angular_Arc_Minute = 9103
   Angular_Arc_Second = 9104
   Angular_Grad = 9105
   Angular_Gon = 9106
   Angular_DMS = 9107
   Angular_DMS_Hemisphere = 9108
   Key 3072 : ProjectedCSTypeGeoKey

   */

  @Override
  public CoordinateReferenceSystem getCRS() {
    return crs;
  }

  /**
   * Returns number of layers (or images) in the main GeoTIFF file
   * @return the total number of layers in the GeoTIFF file
   */
  public int getNumLayers() {
    return reader.getNumLayers();
  }

  public double getPixelScaleX() {
    return g2m.getScaleX();
  }
  public double getPixelScaleY() {
    return g2m.getScaleY();
  }

  @Override
  public void close() throws IOException {
    if (reader != null)
      reader.close();
  }
}
